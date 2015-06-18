class Spatial(object):

  def __init__(self, client):
    self.client = client

  def simplify_heur(self, geom, tol=0.5, topo=False):
    return self.client.post('spatial/simplify', geom, {
      'tolerance': tol, 'topology': topo
    })

  def simplify_dist(self, geom, dist, topo=False):
    return self.client.post('spatial/simplifyWithDist', geom, {
      'distance': dist, 'topology': topo
    })

  def simplify_grid(self, geom, max_levels=6, err_pct=0.1):
    return self.client.post('spatial/simplifyWithGrid', geom, {
      'levels': max_levels, 'disterr': err_pct
    })
