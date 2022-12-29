if __name__ == '__main__':
    import os
    from main import GetRegions
    g_r = GetRegions(r'D:\ma_data\数据处理\core')
    text_regions = [tmp for tmp in g_r.regions]
    for r in text_regions:
        if r not in [tmp.split(".")[0] for tmp in os.listdir(r"D:\ma_data\通勤率\中心到四周通勤密度")]:
            print(r)