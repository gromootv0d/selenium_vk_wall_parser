from selenium import webdriver
from sqlalchemy import *
import time
import json
import threading


def get_all_data():
    my_profile = r"C:\Users\mark\AppData\Local\Google\Chrome\User Data\Default"
    options = webdriver.ChromeOptions()
    options.add_argument("user-data-dir=" + my_profile)
    driver = webdriver.Chrome(chrome_options=options)
    driver.get('https://vk.com/feed')
    return driver
def start_all(driver):
    driver.refresh()
    wall_data = []
    posts = driver.find_elements_by_class_name('feed_row')
    return getting_wall_data(posts, wall_data)

def format_url(urle):
    """ Убираем лишнее в url """
    start_position = urle.find('("')
    end_position = urle.find('")')
    return urle[start_position + 2:end_position]

def get_text(post):
    """ текст из поста """
    try:
        text = post.find_element_by_class_name('wall_post_text').text
    except:
        text = ''
    return text

def get_image(post):
    """ картинка из поста """
    try:
        url = []
        only_post = post.find_element_by_class_name('wall_text')  # only post no comments, support replies
        photo = only_post.find_elements_by_class_name('image_cover')
        for i in range(len(photo)):
            background_image_property = photo[i].value_of_css_property('background-image')
            url_add = format_url(background_image_property)
            url.append(str(url_add))
    except:
        url = []
    return url

def getting_wall_data(posts, wall_data):
    for post in posts:
        try:
            check_add = post.find_elements_by_class_name('wall_text_name_explain_promoted_post')
            if check_add == []:  # проверка на рекламу
                id = (post.find_element_by_class_name('_post').get_attribute('data-post-id'))[1:]
                url = get_image(post)
                text = get_text(post)
                wall_data.append({"image": url, "text": text, "id": id, "link": "https://vk.com/wall-{}".format(id)})
        except:
            pass
    return wall_data



"""записываем в json файлы и добавляем в бд"""
def write_json(wall_data, i):
    """записываем в файл json"""
    with open("f%dJSON.json" % i, "w", encoding='utf8') as write_file:
        json.dump(wall_data, write_file, ensure_ascii=False, indent=2)
    write_file.close()


def write_db(wall1, wall2, wall3):
    """Записываем в бд"""
    engine = create_engine('sqlite:///vk_wall.db', echo=None)
    meta = MetaData()
    wall_post1 = Table(
        'wall_post1', meta,
        Column('id', Integer, primary_key=True),
        Column('image', String),
        Column('text', String),
        Column('vk_id', String),
        Column('link', String),
    )

    wall_post2 = Table(
            'wall_post2', meta,
            Column('id', Integer, primary_key=True),
            Column('image', String),
            Column('text', String),
            Column('vk_id', String),
            Column('link', String),
        )

    wall_post3 = Table(
            'wall_post3', meta,
            Column('id', Integer, primary_key=True),
            Column('image', String),
            Column('text', String),
            Column('vk_id', String),
            Column('link', String),
        )

    meta.create_all(engine)
    conn = engine.connect()

    for i in wall1:
            conn.execute(wall_post1.insert(), [
                {'image': str(i['image'])[2:-2], 'text': str(i['text']), 'vk_id': str(i['id']), 'link': str(i['link'])},
            ])

    for i in wall2:
            conn.execute(wall_post2.insert(), [
                {'image': str(i['image'])[2:-2], 'text': str(i['text']), 'vk_id': str(i['id']), 'link': str(i['link'])},
            ])

    for i in wall3:
            conn.execute(wall_post3.insert(), [
                {'image': str(i['image'])[2:-2], 'text': str(i['text']), 'vk_id': str(i['id']), 'link': str(i['link'])},
            ])

    conn.close()



start = get_all_data() #получение постов и вход вк

time_to_write1 = start_all(start)
time.sleep(1) #ожидаем пока появятся новые посты
time_to_write2 = start_all(start)
time.sleep(1) #ожидаем пока появятся новые посты
time_to_write3 = start_all(start)


"""многопоточная запись в json"""
threading.Thread(target=write_json, args=[time_to_write1, 1]).start()
threading.Thread(target=write_json, args=[time_to_write2, 2]).start()
threading.Thread(target=write_json, args=[time_to_write3, 3]).start()
"""запись в бд"""
write_db(time_to_write1, time_to_write2, time_to_write3)


