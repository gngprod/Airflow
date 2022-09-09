import urllib.request
import os


def d_download_csv(url, out_dir):
    link = f'{url}?format=excel'
    file_name = url.split('fedstat.ru/')[1].replace('/', '_')
    csv_name = os.path.join(out_dir, f'{file_name}.csv')
    # print(link)
    # print(csv_name)
    urllib.request.urlretrieve(link, csv_name)


def d_task_pars_fedstat():
    out_dir = '/home/ubuntuos/fedstat/data'

    url1 = 'https://www.fedstat.ru/indicator/59448'
    d_download_csv(url1, out_dir)
    url2 = 'https://www.fedstat.ru/indicator/42928'
    d_download_csv(url2, out_dir)
    url3 = 'https://www.fedstat.ru/indicator/31452'
    d_download_csv(url3, out_dir)


if __name__ == '__main__':
    out_dir = '/home/ubuntuos/fedstat/data'

    url1 = 'https://www.fedstat.ru/indicator/59448'
    d_download_csv(url1, out_dir)
    url2 = 'https://www.fedstat.ru/indicator/42928'
    d_download_csv(url2, out_dir)
    url3 = 'https://www.fedstat.ru/indicator/31452'
    d_download_csv(url3, out_dir)
