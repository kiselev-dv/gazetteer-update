from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from urlparse import urlparse, parse_qs
import yaml
import sys
import subprocess
import os
import requests
import threading
import logging

class YAMLLoader(yaml.Loader):

    def __init__(self, stream):
        self._root = os.path.split(stream.name)[0]
        super(YAMLLoader, self).__init__(stream)

    def include(self, node):
        filename = self.construct_scalar(node)
        if not filename.startswith('/'):
            filename = os.path.join(self._root, filename)
        with open(filename, 'r') as fp:
            return yaml.load(fp, YAMLLoader)

    def path(self, node):
        filename = self.construct_scalar(node)
        if not filename.startswith('/'):
            filename = os.path.abspath(os.path.join(self._root, filename))
        return filename


YAMLLoader.add_constructor('!include', YAMLLoader.include)
YAMLLoader.add_constructor('!path', YAMLLoader.path)

class Updater(object):
    """
    Updates indices in gazeteer-web with downloading dumps,
    and dumps removal on complete.
    """
    def __init__ (self, config=None):
        """
        Read config
        """
        with open(config, 'r') as fp:
            self.config = yaml.load(fp, YAMLLoader)


    def check_pid(self, pid):
        """
        Check For the existence of a unix pid.
        """
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        else:
            return True

    def execute(self):
        """
        1. Start internal server, to handle callbacks from GW
           For each task:
           1. Download metainfo (timestamps)
           2. Download dump
           3. Call GW update
           4. Wait for import finish with timeout
           5. Clear dump
        """
        pid_path = self.config.get('pid_file', 'gazetteer-update.pid')
        if os.path.isfile(pid_path):
            with open(pid_path, 'r') as pid_file:
                for line in pid_file:
                    old_pid = int(line)
                    if self.check_pid(old_pid):
                        logging.error('Old updater with pid %s is still running. Exit.', old_pid)


        self.pid = os.getpid()
        with open(pid_path, 'w') as pid_file:
            pid_file.write(str(self.pid))

        self.start_server()

        tsFile = self.config.get('timestamps', 'timestamps.html')
        with open(tsFile, "w") as tsf:
                tsf.write("<html><body><pre>")

        for task in self.config.get('tasks', []):
            logging.info('Execute task: %s', task)
            self.execute_task(task)

        self.stop_server()

        # Append to file
        with open(tsFile, "a") as tsf:
                tsf.write("</pre></body></html>")

    def execute_task(self, task):
        """
        1. Download metainfo (timestamps)
        2. Download dump
        3. Call GW update
        4. Wait for import finish with timeout
        5. Clear dump
        """
        self.download_dump(task)
        self.call_import(task)

        if(not self.ImportDone.isSet()):
            h = (task.get('timeout', 1))
            t = h * 60 * 60
            logging.info('Wait for import to be done, timeout %sh (%s sec.)', h, t)
            if (self.ImportDone.wait(t)):
                self.ImportDone.clear()
                logging.info('Done import')
                self.task_done(task)
            else:
                logging.info('Import timed out')

    def task_done(self, task):
        """
        Remove used dump
        """
        dump_path = self.config['base'] + '/dumps/' + task['region'] + '.json.gz'
        logging.info('Remove %s', dump_path)
        os.remove(dump_path)
        logging.info('Task %s done', task.get('region', 'TMP'))

    def call_import(self, task):
        """
        Call GW to import done with callback to local server
        """
        gp = self.config.get('gazetteer_api', [])
        url = gp.get('url', 'http://localhost:8080') + '/location/_import'
        credentials = (gp.get('user', 'admin'),
                       gp.get('pass', ''))
        prms = {}
        if (task.get('drop', False)):
            prms['drop'] = 'true'
            prms['osmdoc'] = 'true'

        region = task.get('region', 'TMP')
        # updater.py internal server callback url
        def_callback_url = 'http://%s:%s' % (self.config.get('host', 'localhost'), self.config.get('port', '8081'))
        callback_base = self.config.get('callback_url', def_callback_url)
        dump_path = self.config.get('base', '/tmp') + '/dumps/' + region + '.json.gz'
        prms['source'] = dump_path
        callback = callback_base + '?region=' + region + "&status={status}" + "&error_msg={error_msg}"

        prms['callback_url'] = callback

        r = requests.get(url, auth=credentials, params=prms)
        try:
            answer = r.json()
            task_state = str(answer['state']);
            logging.info('Task submission state: %s', task_state);
            if (task_state == 'submitted'):
                logging.info('Region %s submited', region)
                self.ImportDone = threading.Event()
            else:
                logging.warning('Region %s submission failed. GW answered: %s', region, answer)
        except ValueError:
            logging.warning('Region %s submission failed. GW answered: %s', region, answer)

    def download_dump(self, task):
        """
        Download dump from src from update cfg
        """
        region = task.get('region', 'TMP')
        dump_path = self.config.get('base', '/tmp') + '/dumps/' + region + '.json.gz'
        dump_src = task.get('dump_src', None)

        if dump_src is not None :
            if os.path.isfile(dump_path) :
                # Dump already exists
                overwrite = task.get('force_dump_reload', self.config.get('force_dump_reload', True))
                if not overwrite  :
                    logging.info("Region %s dump already exists, skip.")
                    return
                else  :
                    logging.info("Region %s dump will be overriden.")
            #Download
            logging.info('Download %s to %s', dump_src, dump_path)
            subprocess.call(['wget', '--no-verbose', dump_src, '-O', dump_path])
            # Write timestamps
            if task.get('dump_ts', None) is not None:
                p = subprocess.Popen(['wget', '--no-verbose', task['dump_ts'], '-O', '-'],
                                 stdout=subprocess.PIPE)
                ts = p.stdout.read()
                with open(self.config.get('timestamps', 'timestamps.html'), "a") as tsf:
                    tsf.write("\n\r")
                    tsf.write(ts)


    def start_server(self):
        """
        Start Embedded server for callbacks
        """
        port = self.config.get('port', 8001)
        self.server = HTTPServer(('localhost', port), GetHandler)

        self.serverThread = ServerThread(self.server)

        logging.info('Start callback server on port %s', port)
        self.serverThread.start()

    def stop_server(self):
        """
        Stop Embedded server for callbacks
        """
        if(self.serverThread.isAlive()):
            self.server.shutdown()

    def on_callback(self, parameters):
        status = parameters.get('status', [None])[0]
        if 'done' != status:
            error_msg = parameters.get('error_msg', [None])[0]
            logging.info("Import task aborted, status %s, message: %s", status, error_msg)

        self.ImportDone.set()

updater = Updater(sys.argv[1])

class GetHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        params = parse_qs(urlparse(self.path).query)
        logging.info('Got callback from gazetteer-web')
        updater.on_callback(params)
        self.send_response(200)

class ServerThread(threading.Thread):

    def __init__(self, server):
        super(ServerThread, self).__init__()
        self.server = server

    def run(self):
        self.setName("Gazetteer import callback webserver")
        self.server.serve_forever()

if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s %(levelname)s - %(message)s', level=logging.INFO)
    updater.execute()
