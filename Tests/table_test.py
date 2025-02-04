
num = 15
figure_number = 0

for i in range(10):
    bit = (num >> i) & 1
    if (bit):
        figure_number += (1 << i)

print(figure_number)