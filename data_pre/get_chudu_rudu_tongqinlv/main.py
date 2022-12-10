import geopandas as gpd
import pandas as pd
import json
import os
from shapely.geometry import Point
from shapely.ops import cascaded_union
from xpinyin import Pinyin
from tqdm import tqdm
import matplotlib.pyplot as plt
import multiprocessing

with open('../../conf.json', 'r', encoding='utf-8') as f:
    conf = json.load(f)
geocode_file = conf["region_code_table"]
od_data = conf["OD_data_directory"]
core = conf["core_shp_directory"]
around = conf["around_shp_directory"]
commute_rate_save_path = conf["commute_rate_save_path"]


class FromNameToCode():
    def __init__(self, file=geocode_file):
        self.geocode_df = pd.read_excel(file)
        #adcode	省份	城市	城市简称	 区县	citycode
        self.shortname_code_dict = {}
        self.quxian_code_dict = {}
        for i, r in self.geocode_df.iterrows():
            if self.shortname_code_dict.get(r['城市简称']):
                self.shortname_code_dict[r['城市简称']].append(r['adcode'])
            else:
                self.shortname_code_dict[r['城市简称']] = [r['adcode']]

            self.quxian_code_dict[r['区县']] = r['adcode']

    # def get_shortname_code_dict(self):
    #     self.shortname_code_dict = {}
    #     for i, r in self.geocode_df.iterrows():
    #         if self.shortname_code_dict.get(r['城市简称']):
    #             self.shortname_code_dict[r['城市简称']].append(r['adcode'])
    #         else:
    #             self.shortname_code_dict[r['城市简称']] = [r['adcode']]

    def nameTocode(self, short_name):
        return self.shortname_code_dict[short_name]

    def quxianTocode(self, quxian):
        return self.quxian_code_dict.get(quxian)


class FromNameToCoreAroundShp():
    def __init__(self, core_path=core, around_path=around, region='上海市'):
        self.core_poly = gpd.read_file(os.path.join(core_path, region[:-1] +'核.shp'))['geometry']
        py = Pinyin()
        pinyin_name = ''.join(py.get_pinyin(region[:-1]).split('-'))
        if pinyin_name == 'shamen':
            pinyin_name = 'xiamen'
        elif pinyin_name == 'zhongqing':
            pinyin_name = 'chongqing'
        self.around_poly = gpd.read_file(os.path.join(around_path, pinyin_name + '.shp'))
        self.around_poly = self.around_poly[self.around_poly['面积'] > 0]
        try:
            self.around_direct = self.around_poly["FIRST_区"].tolist()
        except:
            self.around_direct = self.around_poly["NAME"].tolist()
        self.around_poly = \
            FromNameToCoreAroundShp.union_around_shp(self.around_poly)

    @staticmethod
    def union_around_shp(around_polys_gdf):
        # around_polys_gdf.plot()
        # boundary = gpd.GeoSeries(cascaded_union(around_polys_gdf['geometry']))
        # boundary.plot(color='red')
        # plt.show()
        return cascaded_union(around_polys_gdf['geometry'])

    @staticmethod
    def show_core_shp(core_geo):
        poly = core_geo[0]
        poly.plot()
        plt.show()


class GetRegions():
    def __init__(self, data_directory):
        tmp = []
        for file_name in os.listdir(data_directory):
            if file_name.endswith('核.shp'):
                tmp.append(file_name.split('.')[0][:-1] + '市')
        self.regions = tmp

    def return_regions(self):
        return self.regions


class Region():
    def __init__(self, data_file_directory=r'D:\ma_data\数据处理\核心', region_name='上海'):
        py = Pinyin()
        files = os.listdir(data_file_directory)
        pinyin_name = ''.join(py.get_pinyin(region_name).split('-'))
        self.data = None

    @staticmethod
    def parse_region_around_p_dict(region_around_p_dict):
        lons = []
        lats = []
        pops = []
        pops2core = []
        rates = []
        for k, v in region_around_p_dict.items():
            lons.append(float(k.split('*')[0]))
            lats.append(float(k.split('*')[1]))
            pops.append(v[0])
            pops2core.append(v[1])
            rates.append(v[1] / v[0])
        return pd.DataFrame({'lon': lons, 'lat': lats, 'pop': pops,
                             'pop2core': pops2core, 'commute_rate': rates})

def multi_main(text_regions):
    # g_r = GetRegions(r'D:\ma_data\数据处理\core')
    namecode = FromNameToCode()
    # print(g_r.regions)
    # ['上海市', '乌鲁木齐市', '北京市', ..., '青岛市']
    # 从成都开始 成都的范围有问题
    # text_regions = g_r.regions[:]#6:
    for region in tqdm(text_regions,  position=0, desc="city", leave=False, colour='red', ncols=80):
        print(region)
        adcodes = namecode.nameTocode(region[:-1])
        fntca = FromNameToCoreAroundShp(region=region)
        fntca_core = fntca.core_poly
        fntca_around = fntca.around_poly
        quxian = fntca.around_direct
        # 跑全部的
        # adcodes += [namecode.quxianTocode(qx) for qx in quxian if namecode.quxianTocode(qx) not in adcodes]
        # 仅仅补充那些在fntca_around却不在region的adcodes
        added_adcodes = [namecode.quxianTocode(qx) for qx in quxian if namecode.quxianTocode(qx) not in adcodes]
        adcodes = added_adcodes
        region_around_p_dict = {}

        for code in tqdm(adcodes, position=1, desc="adcode", leave=False, colour='green', ncols=80):
            tmp_file = os.path.join(od_data, str(code) + '.txt')
            try:
                tmp_data = pd.read_csv(tmp_file)

                for i, r in tmp_data.iterrows():
                    if Point(r['lon1'], r['lat1']).intersects(fntca_around) and Point(r['lon2'], r['lat2']).intersects(
                            fntca_core[0]):
                        if region_around_p_dict.get(str(r['lon1']) + '*' + str(r['lat1'])):
                            region_around_p_dict[str(r['lon1']) + '*' + str(r['lat1'])][1] += int(r['cnt'])
                        else:
                            region_around_p_dict[str(r['lon1']) + '*' + str(r['lat1'])] = [int(r['pop']), int(r['cnt'])]
            except:
                pass

        # Region.parse_region_around_p_dict(region_around_p_dict).to_csv(
        #     os.path.join(commute_rate_save_path, region + '.txt'), index=None)
        Region.parse_region_around_p_dict(region_around_p_dict).to_csv(
            os.path.join(commute_rate_save_path, region + '_补充.txt'), index=None)

# def main():
#     g_r = GetRegions(r'D:\ma_data\数据处理\core')
#     namecode = FromNameToCode()
#     # print(g_r.regions)
#     # ['上海市', '乌鲁木齐市', '北京市', ..., '青岛市']
#     # 从成都开始 成都的范围有问题
#     text_regions = g_r.regions[:]#6:
#     for region in tqdm(text_regions,  position=0, desc="city", leave=False, colour='red', ncols=80):
#         print(region)
#         adcodes = namecode.nameTocode(region[:-1])
#         fntca = FromNameToCoreAroundShp(region=region)
#         fntca_core = fntca.core_poly
#         fntca_around = fntca.around_poly
#         quxian = fntca.around_direct
#         # 跑全部的
#         # adcodes += [namecode.quxianTocode(qx) for qx in quxian if namecode.quxianTocode(qx) not in adcodes]
#         # 仅仅补充那些在fntca_around却不在region的adcodes
#         added_adcodes = [namecode.quxianTocode(qx) for qx in quxian if namecode.quxianTocode(qx) not in adcodes]
#         adcodes = added_adcodes
#         region_around_p_dict = {}
#
#         for code in tqdm(adcodes, position=1, desc="adcode", leave=False, colour='green', ncols=80):
#             tmp_file = os.path.join(od_data, str(code) + '.txt')
#             try:
#                 tmp_data = pd.read_csv(tmp_file)
#
#                 for i, r in tmp_data.iterrows():
#                     if Point(r['lon1'], r['lat1']).intersects(fntca_around) and Point(r['lon2'], r['lat2']).intersects(
#                             fntca_core[0]):
#                         if region_around_p_dict.get(str(r['lon1']) + '*' + str(r['lat1'])):
#                             region_around_p_dict[str(r['lon1']) + '*' + str(r['lat1'])][1] += int(r['cnt'])
#                         else:
#                             region_around_p_dict[str(r['lon1']) + '*' + str(r['lat1'])] = [int(r['pop']), int(r['cnt'])]
#             except:
#                 pass
#
#         # Region.parse_region_around_p_dict(region_around_p_dict).to_csv(
#         #     os.path.join(commute_rate_save_path, region + '.txt'), index=None)
#         Region.parse_region_around_p_dict(region_around_p_dict).to_csv(
#             os.path.join(commute_rate_save_path, region + '_补充.txt'), index=None)


if __name__ == '__main__':
    pool = multiprocessing.Pool()
    g_r = GetRegions(r'D:\ma_data\数据处理\core')
    text_regions = [[tmp] for tmp in g_r.regions]
    pool.map(multi_main, text_regions)
    # main()