data = """
id,name,age,salary,address,gender
1,Manish,26,75000,INDIA,m
2,Nikita,23,100000,USA,f
3,Pritam,22,150000,INDIA,m
4,Prantosh,17,200000,JAPAN,m
5,Vikash,31,300000,USA,m
6,Rahul,55,300000,INDIA,m
7,Raju,67,540000,USA,m
8,Praveen,28,70000,JAPAN,m
9,Dev,32,150000,JAPAN,m
10,Sherin,16,25000,RUSSIA,f
11,Ragu,12,35000,INDIA,f
12,Sweta,43,200000,INDIA,f
13,Raushan,48,650000,USA,m
14,Mukesh,36,95000,RUSSIA,m
15,Prakash,52,750000,INDIA,m
"""
with open('new_customer.csv', 'w') as file:
    file.write(data)