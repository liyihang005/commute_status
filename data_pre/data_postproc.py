import pandas as pd
from tqdm import tqdm
import os
import multiprocessing


def main(data_path):
    for file in tqdm(os.listdir(data_path)):
        if file.endswith(".txt"):
            tmp_df = pd.read_csv(os.path.join(data_path, file))
            tmp_df['commute_rate'] = tmp_df['commute_rate'].apply(lambda x: x / 30)
            tmp_df.to_csv(os.path.join(data_path, "通勤率/" + file), index=None)

# 1208 增加融合部分
def integrate_all_commute_data(part1_path=r'D:\ma_data\通勤率\通勤率', part2_path=r'D:\ma_data\通勤率',
                               part3_path=r'D:\ma_data\通勤率\全量通勤率'):
    for file in tqdm(os.listdir(part1_path)):
        if file.endswith(".txt"):
            tmp_df = pd.read_csv(os.path.join(part1_path, file))
            tmp_df2 = pd.read_csv(os.path.join(part2_path, file.split('.')[0] + "_补充" + '.txt'))
            pd.concat([tmp_df[tmp_df['commute_rate'] < 1], tmp_df2[tmp_df2['commute_rate'] < 1]], axis=0).to_csv(
                os.path.join(part3_path, file), index=None)

def integrate_same_orient_data(file, data_path=r'D:\ma_data\通勤率\入度统计',
                               res_path=r'D:\ma_data\通勤率\入度统计\入度'):


    if file.endswith(".txt"):
        tmp_df = pd.read_csv(os.path.join(data_path, file))
        tmp = {}#{d:{o:*, }, }
        lons_d = []
        lats_d = []
        all_od = []
        lons_o = []
        lats_o = []
        ods = []
        for i, r in tmp_df.iterrows():
            if tmp.get(str(r['lon2']) + '*' + str(r['lat2'])):
                if tmp[str(r['lon2']) + '*' + str(r['lat2'])].get(str(r['lon1']) + '*' + str(r['lat1'])):
                    tmp[str(r['lon2']) + '*' + str(r['lat2'])][str(r['lon1']) + '*' + str(r['lat1'])] += r['Indegree']
                else:
                    tmp[str(r['lon2']) + '*' + str(r['lat2'])][str(r['lon1']) + '*' + str(r['lat1'])] = r['Indegree']
            else:
                tmp[str(r['lon2']) + '*' + str(r['lat2'])] = {str(r['lon1']) + '*' + str(r['lat1']): r['Indegree']}

        for k, v in tmp.items():
            cnt = 0
            all_ods = 0
            for kk, vv in v.items():
                cnt += 1
                lons_d.append(float(k.split('*')[0]))
                lats_d.append(float(k.split('*')[1]))
                lons_o.append(float(kk.split('*')[0]))
                lats_o.append(float(kk.split('*')[1]))
                ods.append(int(vv))
                all_ods += int(vv)
            for i in range(cnt):
                all_od.append(all_ods)
        pd.DataFrame({'lon2': lons_d, 'lat2': lats_d, 'SUMIndegree': all_od,
                      'lon1': lons_o, 'lat1': lats_o, 'Indegree': ods}).to_csv(os.path.join(res_path, file), index=None)



if __name__ == '__main__':
    # main(r"D:\ma_data\通勤率")
    # integrate_all_commute_data()
    p = multiprocessing.Pool()
    files = os.listdir(r'D:\ma_data\通勤率\入度统计')
    p.map(integrate_same_orient_data, files)