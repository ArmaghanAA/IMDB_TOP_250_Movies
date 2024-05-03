import requests
from bs4 import BeautifulSoup
import re
from fake_useragent import UserAgent
import numpy as np
import pandas as pd
import time

def request_to_imdb():
    url='https://www.imdb.com/chart/top/?ref_=nv_mv_250'
    try:
        response = requests.get(url, headers=headers,timeout=5)
        first_soup = BeautifulSoup(response.text, 'html.parser')
        status=True
        return status,first_soup
    except requests.exceptions.RequestException as error:
        status= False
        return status,error

def scrapping_first_page(first_soup):
    movies_data = first_soup.find_all('li', 'ipc-metadata-list-summary-item sc-1364e729-0 caNpAE cli-parent')
    movie_urls_list=[]
    for movie in movies_data:
        href = movie.a['href']
        base_url = 'https://www.imdb.com'
        movie_url = base_url + href
        movie_urls_list.append(movie_url)
    return  movie_urls_list


def movie_data(movie_urls_list,movie_list,person_list,cast_list,crew_list,genre_list):
    error_list = []
    movie_table = movie_list
    person_table = person_list
    cast_table = cast_list
    crew_table = crew_list
    genre_table = genre_list
    for url in movie_urls_list:
        movie_id = re.findall(r'/tt(.*)/', url)[0]
        try:
            time.sleep(0.1)
            res= requests.get(url,headers=headers,timeout=5)
            movie_soup = BeautifulSoup(res.text, 'html.parser')

            ### --title--
            movie_title = movie_soup.find('span', 'hero__primary-text').text

            ### --year, parental_guide, runtime--

            ypr = movie_soup.find('ul', 'ipc-inline-list ipc-inline-list--show-dividers sc-d8941411-2 cdJsTz baseAlt')
            ypr_list = []
            for child in ypr.children:
                ypr_list.append(child.text)

                ### --year--
            year = int(ypr_list[0])

                ### --runtime(mins)--
            if ('m' in ypr_list[-1]) and ('h' in ypr_list[-1]):
                duration = re.findall(r'(\d+)h (\d+)m', ypr_list[-1])
                runtime = int(duration[0][0]) * 60 + int(duration[0][1])
            elif('m' not in ypr_list[-1]) and ('h' in ypr_list[-1]):
                duration = re.findall(r'(\d+)h', ypr_list[-1])
                runtime = int(duration[0][0]) * 60
            elif ('m' in ypr_list[-1]) and ('h' not in ypr_list[-1]):
                duration = re.findall(r'(\d+)m', ypr_list[-1])
                runtime = int(duration[0][0])

                ### --parental_guide--
            if len(ypr_list) < 3:
                parental_guide = np.nan
            else:
                parental_guide = ypr_list[1]

            ### --gross_us_canada--
            box_office = movie_soup.find('div', 'sc-a83bf66d-1 gYStnb ipc-page-grid__item ipc-page-grid__item--span-2')
            box_office_text = box_office.text
            gross_us_canada = re.findall(r'Gross US & Canada\$([\d,]+)[a-zA-Z]+', box_office_text)
            if gross_us_canada:
                gross_us_canada = int(gross_us_canada[0].replace(',', ''))
            else:
                gross_us_canada = np.nan

                    #---------movie table-----
            movie_table.append([movie_id,movie_title,year,runtime,parental_guide,gross_us_canada])

            ###----genre-----
            GDWS1 = movie_soup.find('div', 'sc-e226b0e3-10 hbBxmX')

            if GDWS1:
                GDWS=GDWS1
            else:
                GDWS= movie_soup.find('div', 'sc-e226b0e3-10 kRcXhr')
            genres = GDWS.find_all('span', 'ipc-chip__text')

            for each in genres:
                genre_name=each.text
                genre_table.append([movie_id,genre_name])

            ### -----drictore------
            DWS = GDWS.find('div', 'sc-69e49b85-3 dIOekc')
            DWS = DWS.find_all('ul',
                               'ipc-inline-list ipc-inline-list--show-dividers ipc-inline-list--inline ipc-metadata-list-item__list-content baseAlt'
                               , 'ipc-inline-list__item')

            for child in DWS[0].children:
                director_id = re.findall(r'/nm(.*)/', child.a['href'])[0]
                director_name= child.text
                person_table.append([director_id,director_name])
                crew_table.append([movie_id,director_id,'Director'])

            ### -----writer -----
            for child in DWS[1].children:
                writer_id = re.findall(r'/nm(.*)/', child.a['href'])[0]
                writer_name=child.text
                person_table.append([writer_id,writer_name])
                crew_table.append([movie_id,writer_id,'Writer'])

            ### ------star--------
            for child in DWS[2].children:
                star_id = re.findall(r'/nm(.*)/', child.a['href'])[0]
                star_name=child.text
                cast_table.append([movie_id,star_id])
                person_table.append([star_id,star_name])

        except :
            movie_title=np.nan
            year=np.nan
            runtime=np.nan
            parental_guide=np.nan
            gross_us_canada=np.nan
            movie_table.append([movie_id,movie_title,year,runtime,parental_guide,gross_us_canada])

            genre_name=np.nan
            genre_table.append([movie_id, genre_name])

            director_id=np.nan
            director_name=np.nan
            person_table.append([director_id, director_name])
            crew_table.append([movie_id, director_id, np.nan])


            writer_id=np.nan
            writer_name=np.nan
            person_table.append([writer_id, writer_name])
            crew_table.append([movie_id, writer_id, np.nan])

            star_id=np.nan
            person_table.append([star_id, np.nan])
            cast_table.append([movie_id, star_id])

            error_list.append(url)

    return movie_table,person_table,cast_table,crew_table,genre_table,error_list


def df_to_csv(movie,person,cast,crew,genre):
    #-------------movie
    movie_df=pd.DataFrame(movie,columns=['id','title','year','runtime','parental_guide','gross_us_canada'])
    movie_df.drop_duplicates(inplace=True)
    movie_df.drop_duplicates(subset=['id'],keep='last',inplace=True)
    movie_df.to_csv('movie.csv',index=False)
    #------------person
    person_df=pd.DataFrame(person,columns=['id','name'])
    person_df.drop_duplicates(inplace=True)
    person_df.dropna(how='all',inplace=True)
    person_df.to_csv('person.csv',index=False)
    #------------cast
    cast_df=pd.DataFrame(cast,columns=['movie_id','person_id'])
    cast_df.drop_duplicates(inplace=True)
    cast_df.dropna(subset=['person_id'],inplace=True)
    cast_df.to_csv('cast.csv',index=False)
    #------------crew
    crew_df=pd.DataFrame(crew,columns=['movie_id','person_id','role'])
    crew_df.drop_duplicates(inplace=True)
    crew_df.dropna(subset=['person_id','role'],inplace=True)
    crew_df.to_csv('crew.csv',index=False)
    #------------genre
    genre_df=pd.DataFrame(genre,columns=['movie_id','genre'])
    genre_df.drop_duplicates(inplace=True)
    genre_df.dropna(subset=['genre'],inplace=True)
    genre_df.to_csv('genre.csv',index=False)



#--------------main body-----------------#

UA = UserAgent(browsers= ["chrome", "firefox", "safari"],os= ["windows"])
headers={'User-Agent':UA.random ,'Accept-Language':'en-US,en,q=0.5'}

status,output= request_to_imdb()
if status:
    first_soup= output
    movie_urls= scrapping_first_page(first_soup)
    movie,person,cast,crew,genre,remain_urls= movie_data(movie_urls,movie_list=[],person_list=[],cast_list=[],crew_list=[],genre_list=[])
    crawl_status=len(remain_urls)
    while(crawl_status!=0):
        print(f'Try again:\n {remain_urls}')
        movie, person, cast, crew, genre, remain_urls = movie_data(remain_urls,movie,person,cast,crew,genre)
        crawl_status = len(remain_urls)

    print('All data extracted successfully!')
    df_to_csv(movie, person, cast, crew, genre)
else:
    error = output
    print(error)


