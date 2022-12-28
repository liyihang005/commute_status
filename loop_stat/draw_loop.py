# from geopy.distance import geodesic
# from pyproj import Geod
import os
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString, Polygon
from shapely.ops import transform
import pyproj
from xpinyin import Pinyin
import multiprocessing
#TODO  把所有的投影都投到米制上
# https://segmentfault.com/a/1190000039348380


class LoopState():
    def __init__(self, center):
        self.loops = {}
        self.center = center
        self.wgs84 = pyproj.CRS('EPSG:4326')
        # 确定utm坐标系的
        print('EPSG:326{}'.format(int(self.center[0] // 6 + 31)))
        self.utm = pyproj.CRS('EPSG:326{}'.format(int(self.center[0] // 6 + 31)))
        self.project = pyproj.Transformer.from_crs(self.wgs84, self.utm, always_xy=True).transform
        self.utm_center = transform(self.project, Point(self.center[0], self.center[1]))
        self.stat_res = {}
        self.loop_area = {}

    def PointInLoop(self, point, pop2core):
        utm_point = transform(self.project, Point(point[0], point[1]))
        dis = LineString([[utm_point.x, utm_point.y], [self.utm_center.x, self.utm_center.y]]).length
        dis = dis // 1000
        #往self.stat_res中灌入结果，距离（其实就是再某一个半径圆环内部）：[核心区通勤人数1， 核心区通勤人数2，...]
        if self.stat_res.get(dis):
            self.stat_res[dis].append(pop2core)
        else:
            self.stat_res[dis] = [pop2core]

    def LoopRigionOverlap(self, rigion_shp):
        self.around_poly = gpd.read_file(rigion_shp)
        for k in self.stat_res.keys():
            ex_circle = self.utm_center.buffer((k + 1)*1000)
            inner_circle = self.utm_center.buffer(k*1000)
            loop = Polygon(ex_circle.exterior.coords, [inner_circle.exterior.coords])
            self.loop_area[k] = loop.intersection(self.around_poly).area


    # def PointInLoop(self, point):
    #     geod = Geod(ellps="WGS84")
    #     line_string = LineString([self.center, point])
    #     return geod.geometry_length(line_string)

def multi_main(text_regions):
    py = Pinyin()
    pinyin_name = ''.join(py.get_pinyin(text_regions[:-1]))
    if pinyin_name == 'shamen':
        pinyin_name = 'xiamen'
    elif pinyin_name == 'zhongqing':
        pinyin_name = 'chongqing'
    from main import around
    around_poly_shp = os.path.join(around, pinyin_name + '.shp')
    ct = [center_data[center_data["市"] == text_regions]['XCoord'],
          center_data[center_data["市"] == text_regions]['YCoord']]
    ls = LoopState(center=ct)
    commute_data = pd.read_csv('./{}.csv'.format(text_regions))
    for i, r in commute_data.iterrows():
        ls.PointInLoop(point=[r['lon'], r['lat']], pop2core=r['pop2core'])
    ls.LoopRigionOverlap(rigion_shp=around_poly_shp)
    dis = []
    com = []
    for k, v in ls.stat_res.items():
        dis.append(k)
        com.append(v/ls.loop_area[k])
    pd.DataFrame({'dis':dis, 'pop2core/area':com}).to_csv(os.path.join(commute_rate_save_path, text_regions + '.csv'),
                                                          index=None)




if __name__ == '__main__':
    # ls = LoopState(center=[116.398466, 39.919163])
    # ls.PointInLoop(point=[117.251581, 39.128223], pop2core=1000)
    # ls.LoopRigionOverlap(rigion_shp='./***.shp')
    # print(ls.stat_res)
    center_data = gpd.read_file(r'中心点.shp')
    pool = multiprocessing.Pool()
    from main import GetRegions
    g_r = GetRegions(r'D:\ma_data\数据处理\core')
    text_regions = [[tmp] for tmp in g_r.regions]
    pool.map(multi_main, text_regions)