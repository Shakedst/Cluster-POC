import dill
from Count_digits import count

print count([0,2,8,3,2,1,0,2,2,5,2])
print count
x = dill.dumps(count)
print x
#dill.dump(count,'plz.txt')


