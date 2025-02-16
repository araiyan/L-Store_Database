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
from queue import Queue


class Frame:
    '''Each frame inside the bufferpool'''
    def __init__(self):
        self.pin:int = 0
        self.page:Page = None
        self.dirty:bool = False
        '''If the Dirty bit is true we need to write to disk before discarding the frame'''

    def load_page(self, page_path:str):
        page_json_data = json.load(page_path)
        self.page = Page()
        self.page.deserialize(page_json_data)

    def unload_page(self, page_path:str):
        if (self.pin > 0):
            raise MemoryError("Cannot unload a page thats being used by processes")

        if (self.dirty):
            page_json_file = open(page_path, "w", encoding="utf-8")
            page_data = self.page.serialize()
            json.dump(page_data, page_json_file)

        self.dirty = False
        self.page = None

class Bufferpool:
    '''Every access to pages should go through the befferpool'''
    def __init__(self, table_path):
        self.frame_directory = dict()
        '''Frame directory keeps track of page# to frame#'''
        self.frames = list[Frame]
        self.available_frames_queue = Queue(MAX_NUM_FRAME)
        self.unavailable_frames_queue = Queue(MAX_NUM_FRAME)
        
        self.table_path = table_path

        for i in range(MAX_NUM_FRAME):
            self.frames.append(Frame())
            self.available_frames_queue.put(i)

    def get_page(self, page_range_index, record_column, page_index) -> tuple[Page, int] | None:
        '''Returns a Page if the page can be grabbed from disk along with the frame number of that page, 
        otherwise returns None'''

        page_disk_path = os.path.join(f"PageRange_{page_range_index}", f"Page_{record_column}_{page_index}.bin")
        page_frame_num = self.frame_directory.get(page_disk_path, None)

        if (page_frame_num is None):
            # If no frames are available and we were unable to diallocate frames due to P2L then returns None
            if (self.available_frames_queue.empty() and not self.__replacement_policy):
                return None
            
            # Note: block inside get can be used to block transactions until a frame is available (Milestone 3)
            page_frame_num = self.available_frames_queue.get()
            current_frame:Frame = self.frames[page_frame_num]

            full_page_path = os.path.join(self.table_path, page_disk_path)
            current_frame.load_page(full_page_path)
            self.frame_directory[page_disk_path] = page_frame_num

            # marks current frame as being used
            self.unavailable_frames_queue.put(page_frame_num)
            current_frame.pin += 1

            return current_frame.page, page_frame_num
        
        current_frame:Frame = self.frames[page_frame_num]
        current_frame.pin += 1
        return current_frame.page, page_frame_num
    
    def mark_frame_used(self, frame_num):
        '''Use this to close a frame once a page has been used'''
        self.frames[frame_num].pin -= 1
        
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