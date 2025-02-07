import sys
sys.path.append("..")

from lstore.page import Page


new_page = Page()

new_page.write(33512)
new_page.write(4)

# 7's index
index = new_page.num_records
new_page.write(7)
new_page.write(2)
new_page.write(9)
new_page.write(1)
new_page.write(12)
new_page.write(23)
new_page.write(43)
new_page.write(16)

new_page.write_precise(512, 16)
print(new_page.data)
print(new_page.get(index))