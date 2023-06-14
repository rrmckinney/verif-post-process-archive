offset = [60, 100, 30, 45, 78]

x=0
for i in offset:
    while i > 24:
        i = i - 24
        print(i)
    offset[x] = i
    x = x+1


    
print(offset)