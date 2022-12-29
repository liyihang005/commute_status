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
import matplotlib.pyplot as plt
from tqdm import tqdm
#TODO  把所有的投影都投到米制上
# https://segmentfault.com/a/1190000039348380
center_data = gpd.read_file(r'../data/中心点/center.shp')


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
        self.sum_pop = {}
        self.loop_area = {}

    def PointInLoop(self, point, pop2core, pop):
        utm_point = transform(self.project, Point(point[0], point[1]))
        dis = LineString([[utm_point.x, utm_point.y], [self.utm_center.x, self.utm_center.y]]).length
        dis = dis // 1000
        #往self.stat_res中灌入结果，距离（其实就是再某一个半径圆环内部）：[核心区通勤人数1， 核心区通勤人数2，...]
        if self.stat_res.get(dis):
            self.stat_res[dis].append(pop2core)
        else:
            self.stat_res[dis] = [pop2core]
        if self.sum_pop.get(dis):
            self.sum_pop[dis].append(pop)
        else:
            self.sum_pop[dis] = [pop]

    def LoopRigionOverlap(self, rigion_shp):
        self.around_poly = gpd.read_file(rigion_shp)
        self.around_poly = self.around_poly[self.around_poly['面积'] > 0]
        from main import FromNameToCoreAroundShp
        self.around_poly = \
            transform(self.project, FromNameToCoreAroundShp.union_around_shp(self.around_poly)).convex_hull
        for k in self.stat_res.keys():
            ex_circle = self.utm_center.buffer((k + 1)*1000)
            print(k)
            try:
                if k > 0:
                    inner_circle = self.utm_center.buffer(k * 1000)
                    tmp1 = []
                    tmp2 = []
                    for i in range(len(ex_circle.exterior.xy[0])):
                        if (ex_circle.exterior.xy[0][i], ex_circle.exterior.xy[1][i]) not in tmp1:
                            tmp1.append((ex_circle.exterior.xy[0][i], ex_circle.exterior.xy[1][i]))
                    for i in range(len(inner_circle.exterior.xy[0])):
                        if (inner_circle.exterior.xy[0][i], inner_circle.exterior.xy[1][i]) not in tmp2:
                            tmp2.append((inner_circle.exterior.xy[0][i], inner_circle.exterior.xy[1][i]))
                    loop = Polygon(tmp1, [tmp2[::-1]])
                    # loop = Polygon([(ex_circle.exterior.xy[0][i], ex_circle.exterior.xy[1][i]) for i in
                    #                 range(len(ex_circle.exterior.xy[0]))],
                    #                [[(inner_circle.exterior.xy[0][i], inner_circle.exterior.xy[1][i]) for i in
                    #                  range(len(inner_circle.exterior.xy[0]))][::-1]])
                else:
                    loop = Polygon([(ex_circle.exterior.xy[0][i], ex_circle.exterior.xy[1][i]) for i in
                                    range(len(ex_circle.exterior.xy[0]))])
            except:
                p = gpd.GeoSeries(ex_circle)
                p.plot()

                p2 = gpd.GeoSeries(inner_circle)
                p2.plot()
                plt.show()
            # try:
            #     loop = Polygon([(ex_circle.exterior.xy[0][i], ex_circle.exterior.xy[1][i]) for i in range(len(ex_circle.exterior.xy[0]))],
            #                    [[(inner_circle.exterior.xy[0][i], inner_circle.exterior.xy[1][i]) for i in range(len(inner_circle.exterior.xy[0]))][::-1]])
            # except:
            #     print(k)
            #     p = gpd.GeoSeries(inner_circle)
            #     p.plot()
            #     plt.show()
            self.loop_area[k] = loop.intersection(self.around_poly).area
            # try:
            #     self.loop_area[k] = loop.intersection(self.around_poly.conv).area
            # except:
            #     p = gpd.GeoSeries(self.around_poly)
            #     p2 = gpd.GeoSeries(loop)
            #     p.plot()
            #     p2.plot()
            #     plt.show()


    # def PointInLoop(self, point):
    #     geod = Geod(ellps="WGS84")
    #     line_string = LineString([self.center, point])
    #     return geod.geometry_length(line_string)

def multi_main(text_regions):
    py = Pinyin()
    pinyin_name = ''.join(py.get_pinyin(text_regions[:-1]).split('-'))
    if pinyin_name == 'shamen':
        pinyin_name = 'xiamen'
    elif pinyin_name == 'zhongqing':
        pinyin_name = 'chongqing'
    from main import around
    around_poly_shp = os.path.join(around, pinyin_name + '.shp')
    # tmp = center_data
    ct = [center_data[center_data["市"] == text_regions]['XCoord'].values[0],
          center_data[center_data["市"] == text_regions]['YCoord'].values[0]]
    ls = LoopState(center=ct)
    commute_data = pd.read_csv(r'D:\ma_data\通勤率\全量通勤率\{}.txt'.format(text_regions))
    for i, r in tqdm(commute_data.iterrows(), position=0, desc="city", leave=False, colour='red', ncols=80):
        ls.PointInLoop(point=[r['lon'], r['lat']], pop2core=r['pop2core'], pop=r['pop'])
    ls.LoopRigionOverlap(rigion_shp=around_poly_shp)
    dis = []
    com = []
    for k, v in ls.stat_res.items():
        dis.append(k)
        # 统计对象改为均值
        # com.append(sum(v) / len(v))
        # 统计对象为总通勤率 总的核心通勤 除以 总的 通勤
        com.append(sum(v) / sum(ls.sum_pop[k]) / 30)
        #统计对象为总的核心通勤 除以 圆环面积
        # com.append(sum(v)/ls.loop_area[k])
    # pd.DataFrame({'dis': dis, 'pop2core/area': com}).to_csv(os.path.join(r"D:\ma_data\通勤率\中心到四周通勤密度", text_regions + '.csv'),
    #                                                       index=None)
    pd.DataFrame({'dis': dis, 'sum(pop2core)/sum(pop)': com}).sort_values(by='dis').to_csv(
        os.path.join(r"D:\ma_data\通勤率\中心到四周通勤密度", text_regions + '.csv'),
        index=None)


if __name__ == '__main__':

    # ls = LoopState(center=[116.398466, 39.919163])
    # ls.PointInLoop(point=[117.251581, 39.128223], pop2core=1000)
    # ls.LoopRigionOverlap(rigion_shp='./***.shp')
    # print(ls.stat_res)

    pool = multiprocessing.Pool()
    from main import GetRegions
    g_r = GetRegions(r'D:\ma_data\数据处理\core')
    text_regions = [tmp for tmp in g_r.regions]
    pool.map(multi_main, text_regions)

    #  这两个城市的数据存在空的子块
    # multi_main("成都市")
    # multi_main("贵阳市")