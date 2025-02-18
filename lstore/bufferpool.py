'''
Every access to Memory should go through the bufferpool
The format for the page placement inside the disk is as follows
DB Directory: Folder
    -> TableName: Folder
        -> PageRange_{page_range_index}: Folder
            -> Page_{record_column}_{page_index}.bin: File
'''


from lstore.config import MAX_NUM_FRAME
from lstore.page import Page
import os
import json
import threading
from queue import Queue
from typing import List, Union


class Frame:
    '''Each frame inside the bufferpool'''
    def __init__(self):
        self.pin:int = 0
        self.page:Page = None
        self.page_path = None
        self.dirty:bool = False
        '''If the Dirty bit is true we need to write to disk before discarding the frame'''

        self._pin_lock = threading.Lock()
        self._write_lock = threading.Lock()

    def load_page(self, page_path:str):
        '''Loads a page from disk to memory
        If the page is not found then it creates a new page'''

        self._write_lock.acquire()

        self.page = Page()
        self.page_path = page_path
        if os.path.exists(page_path):
            with open(page_path, "r", encoding="utf-8") as page_file:
                page_json_data = json.load(page_file)
            self.page.deserialize(page_json_data)
        else:
            with self._write_lock:
                self.dirty = True

        self._write_lock.release()

    def unload_page(self):
        if (self.pin > 0):
            raise MemoryError("Cannot unload a page thats being used by processes")
        
        self._write_lock.acquire()

        if (self.dirty):
            with open(self.page_path, "w", encoding="utf-8") as page_json_file:
                page_data = self.page.serialize()
                json.dump(page_data, page_json_file)

        self.dirty = False
        self.page = None
        self.page_path = None

        self._write_lock.release()

    def write_precise_with_lock(self, slot_index, value):
        '''Writes a value to a page slot with a lock'''
        with self._write_lock:
            self.dirty = True
            self.page.write_precise(slot_index, value)

    def write_with_lock(self, value) -> int:
        '''Writes a value to a page with a lock'''
        with self._write_lock:
            self.dirty = True
            return self.page.write(value)

    def increment_pin(self):
        '''Increments the pin count'''
        with self._pin_lock:
            self.pin += 1

    def decrement_pin(self):
        '''Decrements the pin count'''
        with self._pin_lock:
            self.pin -= 1

class Bufferpool:
    '''Every access to pages should go through the bufferpool'''
    def __init__(self, table_path):
        self.frame_directory = dict()
        '''Frame directory keeps track of page# to frame#'''
        self.frames:List[Frame] = []
        self.available_frames_queue = Queue(MAX_NUM_FRAME)
        self.unavailable_frames_queue = Queue(MAX_NUM_FRAME)
        
        self.table_path = table_path

        for i in range(MAX_NUM_FRAME):
            self.frames.append(Frame())
            self.available_frames_queue.put(i)

    def get_page_frame(self, page_range_index, record_column, page_index) -> Union[Frame, None]:
        '''Returns a Frame of a page if the page can be grabbed from disk, 
        otherwise returns None'''

        page_disk_path = self.get_page_path(page_range_index, record_column, page_index)
        page_frame_num = self.frame_directory.get(page_disk_path, None)

        if (page_frame_num is None):
            # If no frames are available and we were unable to diallocate frames due to lock then returns None
            if (self.available_frames_queue.empty() and not self.__replacement_policy()):
                return None
            
            current_frame:Frame = self.__load_new_frame(page_disk_path)
            return current_frame
        
        current_frame:Frame = self.frames[page_frame_num]
        return current_frame
    
    def get_page_slot(self, page_range_index, record_column, page_index, slot_index) -> Union[int, None]:
        '''Returns the value within a page if the page can be grabbed from disk,
        otherwise returns None'''
        page_disk_path = self.get_page_path(page_range_index, record_column, page_index)
        page_frame_num = self.frame_directory.get(page_disk_path, None)

        if (page_frame_num is None):
            if (self.available_frames_queue.empty() and not self.__replacement_policy()):
                return None

            current_frame:Frame = self.__load_new_frame(page_disk_path)
            return current_frame.page.get(slot_index)

        current_frame:Frame = self.frames[page_frame_num]
        current_frame.increment_pin()
        return current_frame.page.get(slot_index)
    
    def write_page_slot(self, page_range_index, record_column, page_index, slot_index, value) -> bool:
        '''Writes a value to a page slot'''
        page_disk_path = self.get_page_path(page_range_index, record_column, page_index)
        page_frame_num = self.frame_directory.get(page_disk_path, None)

        if (page_frame_num is None):
            if (self.available_frames_queue.empty() and not self.__replacement_policy()):
                return False

            current_frame:Frame = self.__load_new_frame(page_disk_path)
            current_frame.write_precise_with_lock(slot_index, value)
            current_frame.decrement_pin()
            return True

        current_frame:Frame = self.frames[page_frame_num]
        current_frame.increment_pin()
        current_frame.write_precise_with_lock(slot_index, value)
        current_frame.decrement_pin()
        return True
    
    def get_page_frame_num(self, page_range_index, record_column, page_index) -> Union[int, None]:
        '''Returns the frame number of the page if the page is in memory, otherwise returns None'''
        page_disk_path = self.get_page_path(page_range_index, record_column, page_index)
        return self.frame_directory.get(page_disk_path, None)

    def get_page_path(self, page_range_index, record_column, page_index) -> str:
        '''Returns the path of the page'''
        return os.path.join(f"PageRange_{page_range_index}", f"Page_{record_column}_{page_index}.bin")
    
    def mark_frame_used(self, frame_num):
        '''Use this to close a frame once a page has been used'''
        self.frames[frame_num].decrement_pin()

    def __load_new_frame(self, page_path:str) -> Frame:
        '''Loads a new frame into the bufferpool'''
        
        # Note: block inside get can be used to block transactions until a frame is available (Milestone 3)
        page_frame_num = self.available_frames_queue.get()
        current_frame:Frame = self.frames[page_frame_num]
        current_frame.increment_pin()

        full_page_path = os.path.join(self.table_path, page_path)
        current_frame.load_page(full_page_path)
        self.frame_directory[page_path] = page_frame_num

        self.unavailable_frames_queue.put(page_frame_num)

        return current_frame
        
    def __replacement_policy(self) -> bool:
        '''
        Using LRU Policy
        Returns true if we were properly able to allocate new space for a frame
        '''
        num_used_frames = self.unavailable_frames_queue.qsize()
        replacement_success = False

        for _ in range(num_used_frames):
            frame_num = self.unavailable_frames_queue.get()
            current_frame:Frame = self.frames[frame_num]

            if (current_frame.pin == 0):
                # If the frame is not being used by any processes then we can deallocate it
                current_frame.unload_page()
                self.available_frames_queue.put(frame_num)
                replacement_success = True
            else:
                # If the frame is being used by a process then we put it back in the queue
                self.unavailable_frames_queue.put(frame_num)

        return replacement_success