import sys
sys.path.append("..")
import unittest
from lstore.page import Page

class TestPageSerializeDeserialize(unittest.TestCase):

  def test(self):
    '''Test that serializing and then deserializing results in the same Page object'''
    # instantiate page
    page = Page()

    page.num_records = 5
    page.data[:5] = bytearray([1, 2, 3, 4, 5])

    serialized_data = page.serialize()

    self.assertIn("num_records", serialized_data)
    self.assertIn("data", serialized_data)

    # new page instance
    new_page = Page()

    # everything below checks that deserialize is correctly retaining the data we input
    new_page.deserialize(serialized_data)

    self.assertEqual(new_page.num_records, page.num_records)
    self.assertEqual(new_page.data[:5], bytearray([1, 2, 3, 4, 5]))


if __name__ == '__main__':
  unittest.main()

