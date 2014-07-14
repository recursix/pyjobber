'''
Dummy example to show how you can use mpi to compute the sum of elements in a MPI way.
'''

from mpi4py import MPI #@UnresolvedImport
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

# Python 2 and 3 support
try:
    range = xrange
except NameError:
    pass


def parallelSum(end):
    return sum(range(rank,end, size))


if __name__ == "__main__":
    n = 10
    s = parallelSum(n)
    assert s == (n*(n-1))/2
    
