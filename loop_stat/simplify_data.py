import pandas as pd
import os

def simplify_data(file, output_file):
    data = pd.read_csv(file)
    lon = [data.iloc[0, 0]]
    lat = [data.iloc[0, 1]]
    pop2core = [data.iloc[0, 3]]
    tmp = data.iloc[0, 0] + data.iloc[0, 1]
    for i, r in data.iterrows():
        if tmp == r['lon'] + r['lat']:
            continue
        else:
            lon.append(r['lon'])
            lat.append(r['lat'])
            pop2core.append(r['pop2core'])
            tmp = r['lon'] + r['lat']
    pd.DataFrame({'lon':lon, 'lat':lat, 'pop2core':pop2core}).to_csv(output_file, index=None)


def simplify_datas(dir, output_dir):
    for file in os.listdir(dir):
        simplify_data(os.path.join(dir, file), os.path.join(output_dir, file))


if __name__ == '__main__':
    simplify_datas()