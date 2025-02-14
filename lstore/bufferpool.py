from lstore.config import MAX_NUM_FRAME
from lstore.page import Page
import json

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

