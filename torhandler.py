#!/usr/bin/env python
import os
import stem.process
import logging
from multiprocessing import Process, JoinableQueue as Queue

logging.basicConfig(level=logging.DEBUG)

class TorHandler(object):
    """
    Instantiate and control tor processes via the stem library.
    Each tor process is binded to a given socks port and can be controlled
    via its own unique control port.
    """
    def __init__(self, num_tor_instances=1, base_socks_port = 8000):
        """
        Initialize TorHandle configuration options.

        Parameters
        ----------
        num_tor_instances : The number of tor instances to be launched in
                            parallel. Defaults to 1
        base_socks_port   : The first socks port that tor processes use.
                            The total number of socks ports equals num_tor_instances.
                            Ports are consecutive, i.e., for 3 tor instances,
                            ports 8000, 8001 and 8002 are used.

        """
        if not isinstance(num_tor_instances, int):
            raise ValueError()
        if not isinstance(base_socks_port, int):
            raise ValueError()
        self.num_tor_instances = num_tor_instances
        self.tor_processes_queue = Queue()
        self.base_socks_port = base_socks_port
        self.base_control_port = base_socks_port + num_tor_instances
        self.data_directory_prefix = os.path.expanduser("~/.tor/tor_")

    def launch(self):
        """
        Launches multiple tor instances using the options
        specified in the class constructor.
        """
        def _process_launcher(process_index, queue):
            """
            Helper to launch tor processes via stem.

            Parameters
            ----------
            process_index : An integer number between 0 and
                            self.num_tor_instances - 1.
            queue         : An instance of processing.JoinableQueue
            """
            socks_port = self.base_socks_port + process_index
            control_port = self.base_control_port + process_index
            # must specify a different data directory for each tor process
            # otherwise you would get an error. See
            # https://gitweb.torproject.org/stem.git/commitdiff/56aac96d6213f28a6b597c640affc7a5a963bf75
            data_directory = "%ss%ic%i" % (self.data_directory_prefix, socks_port, control_port)
            # it is better to preserve cache directories since they significantly speed-up
            # circuit creation
            if not os.path.exists(data_directory):
                os.makedirs(data_directory)
            logging.debug('launching tor: socks_port: %i control_port: %i', socks_port, control_port)
            tor_process = stem.process.launch_tor_with_config(
                config = {
                    'SocksPort': str(socks_port),
                    'ControlPort' : str(control_port),
                    'DataDirectory' : data_directory,
# stuff to keep contacting an host from the same exit node
                    'TrackHostExits' : '.', # match everything
                    'TrackHostExitsExpire' : '1800', # 30 mins
#                    'MaxCircuitDirtiness'  : '1800',
#                    'CircuitStreamTimeout' : '1800',
#                    'MaxClientCircuitsPending' : '1',
# done, let's continue with the config
                    'DisableDebuggerAttachment' : "0", #see https://stem.torproject.org/tutorials/east_of_the_sun.html
                    'Log': [
                        'NOTICE file %s/tor_notice_log.txt'% data_directory,
                        'ERR file %s/tor_error_log.txt' % data_directory,
                    ],
                },
                timeout=None # setting a timeout causes a ValueError: signal only works in main thread
#                take_ownership=True # kills tor instances if the main process is terminated
            )
            queue.put(tor_process)
            logging.debug('tor process with pid %i successfully launched' % tor_process.pid)

        # launch multiple tor instances using threads
        processes = list()
        for process_index in range(self.num_tor_instances):
            pro = Process(target=_process_launcher, args=[process_index,self.tor_processes_queue])
            pro.start()
            processes.append(pro)
        for pro in processes:
            pro.join()
        logging.debug("%i tor instances successfully launched" % self.num_tor_instances)

    def kill(self):
        """
        Kills previously launched tor instances
        """
        def _process_killer(queue):
            """
            Helper function to kill tor processes.

            Parameters
            ----------
            queue : A shared multiprocessing.JoinableQueue instance
            """
            tor_process = queue.get()
            logging.debug("killing tor process with pid: %i", tor_process.pid)
            tor_process.kill()
            queue.task_done()
            queue.close()
            logging.debug("tor process with pid %i successfully killed", tor_process.pid)

        for _ in range(self.num_tor_instances):
            p = Process(target=_process_killer, args=[self.tor_processes_queue])
            p.daemon = True
            p.start()
        # killer processes have been daemonized. So we can wait here
        # with a join on the queue. The join unlocks after the last call to the task_done by
        # the killer process
        self.tor_processes_queue.join()
        self.tor_processes_queue.close()
    

if __name__ == "__main__":
    th = TorHandler(10)
    th.launch()
    a = raw_input()
    th.kill()
