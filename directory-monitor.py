#!/usr/bin/env python3
"""
Sample code to show directory monitoring in Python with logic to wait for file copying to finish
"""

__author__ = "Ryan Campbell"
__version__ = "0.1.0"

from dataclasses import dataclass
import datetime
from datetime import timedelta

import argparse
import os
import threading
import time
from logzero import logger

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

FILE_QUEUE = {}

@dataclass
class FileQueueItem:
    """
    Generic message to test the SDK
    """
    file_path: str = ""
    """
    Current file path
    """
    file_size: float = 0
    """
    File size in bytes
    """
    file_name: str = ""
    """
    File name
    """
    file_extention: str = ""
    """
    File extension
    """
    next_poll: datetime = datetime.datetime.min
    """
    Next timestamp to check if this file is ready
    """

    #Remove python's scaffolding of the init so we don't have to force default values
    def __init__(self):
        pass

class DirectoryWatcher:
    """
    Monitors a given directory for any changes and fires callback when found
    """

    def __init__(self, monitor_directory: str, call_back):
        self.observer = Observer()
        self.monitor_directory = monitor_directory
        self.observer.schedule(
            call_back, self.monitor_directory, recursive=True)
        self.observer.start()




class FileProcessor(FileSystemEventHandler):
    """
    Handles events when directories are updated and processes the files after the polling time
    """

    def __init__(self, polling_time_secs:int):
        self.polling_time_secs = polling_time_secs

        self.processor = threading.Thread(target=self.process_file)
        self.processor.daemon = True
        self.processor.start()

    def on_any_event(self, event):
        """
        Callback function from directory watcher to alert when a new file is written to the directory
        """
        # Only take action where a modified event occurred on a file
        if event.is_directory or event.event_type != 'modified':
            return

        # File already exists in the queue.  Let the polling handle the update
        if event.src_path in FILE_QUEUE:
            return

        logger.info("'%s' updated.  Adding to queue", event.src_path)

        _, file_extension = os.path.splitext(event.src_path)

        queued_file:FileQueueItem = FileQueueItem()
        queued_file.file_path = event.src_path
        queued_file.file_size = os.path.getsize(event.src_path)
        queued_file.file_name = os.path.basename(event.src_path)
        queued_file.file_extention = file_extension.lower()
        queued_file.next_poll = datetime.datetime.now() + timedelta(seconds = self.polling_time_secs)

        # Add the file to the queue and start polling
        FILE_QUEUE[event.src_path] = queued_file

    def process_file(self):
        """
        Loop through the staged files and check for any files that are ready to be processed
        """
        while True:
            current_time = datetime.datetime.now()
            current_items = dict(FILE_QUEUE)  #Duplicate our dictionary for this pass so we can adjust it
            for queue_file_path, queued_file in current_items.items():

                #Cast object to be the classed item for strong typing in VSCode
                queued_file:FileQueueItem = queued_file

                #We haven't had enough time pass since the last polling.  Go to next item in list
                if queued_file.next_poll > current_time:
                    continue

                current_file_size = os.path.getsize(queue_file_path)

                #The file is still writing.  Updating the polling time so we can check back later
                if current_file_size != queued_file.file_size:
                    queued_file.file_size = current_file_size
                    queued_file.next_poll = current_time  + timedelta(seconds = self.polling_time_secs)
                    FILE_QUEUE[queue_file_path] = queued_file
                    continue

                #Any files that get this far are ready to be processed.  Remove from the staging queue
                FILE_QUEUE.pop(queue_file_path)

                #This is where you'd take action on the file.  Writing to output directory as an example
                logger.info("'%s' is ready for processing", queued_file.file_path)

            #Give the system a breather before we loop and check again
            time.sleep(1)


def main():
    """ Main entry point of the app """

    file_processor = FileProcessor(polling_time_secs=args.polling_time_secs)

    for directory in args.directories_to_monitor:
        if(os.path.exists(directory)):
            DirectoryWatcher(directory, file_processor)
            logger.info("Adding monitor to '%s'", directory)
        else:
            logger.error("Directory '%s' does not exist.  Skipping for monitor", directory)

    #Keep the app running
    while True:
        time.sleep(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument('--polling-time',
                        dest='polling_time_secs',
                        help='The number of seconds between filesize polls to determine when the file copy is complete.  Add time for slow copy scenarios, or reduce for fast copy scenarios.  Defaults to 2 seconds',
                        type=int,
                        default=2
                        )

    parser.add_argument('--directories-to-monitor',
                        dest='directories_to_monitor',
                        help='List of directories to monitor for new file updates',
                        nargs="*",
                        )

    args = parser.parse_args()
    print("------------------------------------------")
    print("Directory Monitor")
    print("Monitors a set of directories for any updates")
    now = datetime.datetime.now()
    print("Start time: ", now.strftime('%Y-%m-%d %H:%M:%S'))
    print("CONFIG VALUES: ")

    TEMPLATE = "{0:25}{1}"  # column widths: 15, n

    for key, value in sorted(vars(args).items()):
        print(TEMPLATE.format(key, value))


    main()
