from random import gauss

def gaussrange(n, mu=None, sigma=None):
    """sample integer from from gaussian(mu,sigma) in range [0,n)."""
    mu = mu if mu is not None else n/2
    sigma = sigma if sigma is not None else n/10
    return max(0, min(n-1, int(gauss(mu, sigma))))
