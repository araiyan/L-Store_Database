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
    page.write_precise(0, 1)
    page.write_precise(1, 2)
    page.write_precise(2, 3)
    page.write_precise(3, 4)
    page.write_precise(4, 5)

    serialized_data = page.serialize()

    self.assertIn("num_records", serialized_data)
    self.assertIn("data", serialized_data)

    # new page instance
    new_page = Page()

    # everything below checks that deserialize is correctly retaining the data we input
    new_page.deserialize(serialized_data)

    self.assertEqual(new_page.num_records, page.num_records)
    
    # loop over indicies (0 - 4) and check that values at those locations are correct
    for i, value in enumerate([1, 2, 3, 4, 5]):
      self.assertEqual(new_page.get(i), value)


if __name__ == '__main__':
  unittest.main()

