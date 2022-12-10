from main import *

with open('../../conf.json', 'r', encoding='utf-8') as f:
    conf = json.load(f)
geocode_file = conf["region_code_table"]
od_data = conf["OD_data_directory"]
core = conf["core_shp_directory"]
around = conf["around_shp_directory"]
commute_reverse_save_path = "D:\\ma_data\\通勤率\\入度统计"




def parse_region_around_p_dict_for_ind(region_around_p_dict):
    lons_d = []
    lats_d = []
    all_od = []
    lons_o = []
    lats_o = []
    ods = []
    for k, v in region_around_p_dict.items():
        cnt = 0
        all_ods = 0
        for vv in v:
            cnt += 1
            lons_d.append(float(k.split('*')[0]))
            lats_d.append(float(k.split('*')[1]))
            lons_o.append(float(vv[0]))
            lats_o.append(float(vv[1]))
            ods.append(int(vv[2]))
            all_ods += int(vv[2])
        for i in range(cnt):
            all_od.append(all_ods)

    return pd.DataFrame({'lon2': lons_d, 'lat2': lats_d, 'SUMIndegree': all_od,
                         'lon1': lons_o, 'lat1': lats_o, 'Indegree': ods})


def main():
    g_r = GetRegions(r'D:\ma_data\数据处理\core')
    namecode = FromNameToCode()
    print(g_r.regions)
    # ['上海市', '乌鲁木齐市', '北京市', ..., '青岛市']
    text_regions = g_r.regions[12:18]
    for region in tqdm(text_regions, position=0, desc="city", leave=False, colour='red', ncols=80):
        print(region)
        adcodes = namecode.nameTocode(region[:-1])
        fntca = FromNameToCoreAroundShp(region=region)
        fntca_core = fntca.core_poly
        fntca_around = fntca.around_poly
        region_around_p_dict = {}
        quxian = fntca.around_direct
        adcodes += [namecode.quxianTocode(qx) for qx in quxian if namecode.quxianTocode(qx) not in adcodes]
        for code in tqdm(adcodes,  position=1, desc="adcode", leave=False, colour='green', ncols=80):
            tmp_file = os.path.join(od_data, str(code) + '.txt')
            tmp_data = pd.read_csv(tmp_file)

            for i, r in tmp_data.iterrows():
                if Point(r['lon1'], r['lat1']).intersects(fntca_around) and Point(r['lon2'], r['lat2']).intersects(fntca_around):
                    if region_around_p_dict.get(str(r['lon2']) + '*' + str(r['lat2'])):
                        region_around_p_dict[str(r['lon2']) + '*' + str(r['lat2'])].append([r['lon1'], r['lat1'], r['cnt']])
                    else:
                        region_around_p_dict[str(r['lon2']) + '*' + str(r['lat2'])] = [[r['lon1'], r['lat1'], r['cnt']]]

        parse_region_around_p_dict_for_ind(region_around_p_dict).to_csv(
            os.path.join(commute_reverse_save_path, region + '.txt'), index=None)


if __name__ == '__main__':
    main()
