###Given a classroom of n students, how many groups of 4 can we create?
###For this, consider the difference between the % operator and the // operator

##c(n,4)=n!/4!(n-4)!

#without NumPy

import math
def factorial(n,k):
    return math.factorial(n) // (math.factorial(k) * math.factorial(n-k))

#with NumPy