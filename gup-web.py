from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
import urlparse
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
    
    def __init__ (self, config=None):
        with open(config, 'r') as fp:
            self.config = yaml.load(fp, YAMLLoader)
            
    def execute(self):
        self.start_server()
        
        tsFile = self.config.get('timestamps', 'timestamps.html')
        with open(tsFile, "w") as tsf:
                tsf.write("<html><body><pre>")
        
        for task in self.config.get('tasks', []):
            logging.info('Execute task: %s', task)
            self.execute_task(task)
        
        self.stop_server()
        
        with open(tsFile, "a") as tsf:
                tsf.write("</pre></body></html>")    
     
    def execute_task(self, task):
        self.download_dump(task)
        self.call_import(task)
        
        if(not self.ImportDone.isSet()):
            t = (task.get('timeout', 1)) *  60 * 60
            logging.info('Wait for import to be done, timeout %s sec.', t)
            if (self.ImportDone.wait(t)):
                self.ImportDone.clear()
                logging.info('Done import')
                self.task_done(task)
            else:
                logging.info('Import timed out')
    
    def task_done(self, task):
        dump_path = self.config['base'] + '/dumps/' + task['region'] + '.json.gz'
        logging.info('Remove %s', dump_path)
        os.remove(dump_path)
        logging.info('Task %s done', task.get('region', 'TMP'))
        
    def call_import(self, task):
        gp = self.config.get('gazetteer_api', [])
        url = gp.get('url', 'http://localhost:8080') + '/location/_import'
        credentials = (gp.get('user', 'admin'), 
                       go.get('pass', ''))
        prms = {}
        if (task.get('drop', false)):
            prms['drop'] = 'true'
            prms['osmdoc'] = 'true'
        
        region = task.get('region', 'TMP')
        callback_base = self.config.get('callback_url', 'http://localhost:8001')
        dump_path = self.config.get('base', '/tmp') + '/dumps/' + region + '.json.gz'
        prms['source'] = dump_path
        callback = callback_base + '?region=' + region
        prms['callback_url'] = callback 
        
        r = requests.get(url, auth=credentials, params=prms)
        answer = r.json()
        if (answer.get('state', '') == 'submited'):
            self.ImportDone = threading.Event()
        
    def download_dump(self, task):
        region = task.get('region', 'TMP')
        dump_path = self.config.get('base', '/tmp') + '/dumps/' + region + '.json.gz'
        dump_src = task.get('dump_src', None)
        
        if dump_src is not None :
            if not os.path.isfile(dump_path) :
                # Dump already exists
                if not task.get('force_dump_reload', 
                                self.config.get('force_dump_reload', True)) :
                    return
            
            logging.info('Download %s to %s', dump_src, dump_path)        
            subprocess.call(['wget', '--no-verbose', dump_src, '-O', dump_path])
            
            if task.get('dump_ts', None) is not None:
                p = subprocess.Popen(['wget', '--no-verbose', task['dump_ts'], '-O', '-'], 
                                 stdout=subprocess.PIPE)
                
                ts = p.stdout.read()
                with open(self.config.get('timestamps', 'timestamps.html'), "a") as tsf:
                    tsf.write("\n\r")
                    tsf.write(ts)
                
        
    def start_server(self):
        self.serverThread = ServerThread(self)
        self.serverThread.start()
     
    def stop_server(self):
        if(self.serverThread.isAlive()):
            self.serverThread.server.shutdown()

class MyServer(HTTPServer):
    
    def __init__(self, updater, args, handler):
        HTTPServer.__init__(self, args, handler)
        self.updater = updater

class ServerThread(threading.Thread):
    
    def __init__(self, updater):
        super(ServerThread, self).__init__()
        self.updater = updater
        self.setName("Gazetteer import callback webserver")
        
    def run(self):
        port = self.updater.config.get('port', 8001)
        logging.info('Start server on %s', port)    
        self.server = MyServer(self.updater, ('localhost', port), GetHandler)
        self.server.serve_forever()
            
        
class GetHandler(BaseHTTPRequestHandler):
    
    def do_GET(self):
        parsed_path = urlparse.urlparse(self.path)
        self.send_response(200)
        self.server.updater.ImportDone.set()
                    
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    Updater(sys.argv[1]).execute()
    