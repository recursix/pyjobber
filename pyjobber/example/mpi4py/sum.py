'''
Dummy example to show how you can use mpi to compute the sum of elements in a MPI way.
'''

from mpi4py import MPI #@UnresolvedImport
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()



def parallelSum(end):
    return sum(xrange(rank,end, size))
    


if __name__ == "__main__":
    n = 10
    s = parallelSum(n)
    assert s == (n*(n-1))/2
    