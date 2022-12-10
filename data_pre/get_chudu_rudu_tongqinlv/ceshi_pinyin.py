from main import GetRegions
from xpinyin import Pinyin

def new_main():
    g_r = GetRegions(r'D:\ma_data\数据处理\core')
    p = Pinyin()
    # print(g_r.regions)
    # ['上海市', '乌鲁木齐市', '北京市', ..., '青岛市']
    text_regions = g_r.regions[1:]
    for region in text_regions:
        print(region)
        print(''.join(p.get_pinyin(region).split('-')))

if __name__ == '__main__':
    new_main()