import requests
ucar_url_base = "https://data.cosmic.ucar.edu/gnss-ro/"

useful_subfolders=['postProc','repro2013','repro2016']
excluded_levels = ['level1a']

ucar_site_dict = {}

def get_subfolders(url):
    list = []
    r = requests.get(url)
    curl_lines = r.text.split('\n')
    for line in curl_lines:
        if "href" in line and line.split('"')[1][:-1] != '..':
            list.append(line.split('"')[1][:-1])
    return list

#get ucar missions
missions = get_subfolders(ucar_url_base)
for m in missions:
    ucar_site_dict[m]={}
    subfolder = get_subfolders(f'{ucar_url_base}/{m}')
    for s in subfolder:
        if s in useful_subfolders:
            ucar_site_dict[m][s]={}
            levels = get_subfolders(f'{ucar_url_base}/{m}/{s}')
            for l in levels:
                if l not in excluded_levels:
                    ucar_site_dict[m][s][l]={}
                    years = get_subfolders(f'{ucar_url_base}/{m}/{s}/{l}')
                    #print(m,s,l,years)
                    for y in years:

                        days = get_subfolders(f'{ucar_url_base}/{m}/{s}/{l}/{y}')
                        ucar_site_dict[m][s][l][y]=len(days)
                        print(m,s,l,y,len(days))
                else:
                    #print(m,s,l)
                    pass
        else:
            #print(m,s)
            pass


#print(ucar_site_dict)
