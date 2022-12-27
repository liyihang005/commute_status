# from geopy.distance import geodesic
from pyproj import Geod
from shapely.geometry import Point, LineString
#TODO  把所有的投影都投到米制上
# https://segmentfault.com/a/1190000039348380

class LoopState():
    def __init__(self, center):
        self.loops = {}
        self.center = center

    def PointInLoop(self, point):
        geod = Geod(ellps="WGS84")
        line_string = LineString([self.center, point])
        return geod.geometry_length(line_string)

