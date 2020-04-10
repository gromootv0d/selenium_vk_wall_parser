from selenium import webdriver
from sqlalchemy import *
import time
import json
import threading

class open_vk():
    """ открываем вк """
    def __init__(self):
        self.my_profile = r"C:\Users\mark\AppData\Local\Google\Chrome\User Data\Default"
        self.options = webdriver.ChromeOptions()
        self.options.add_argument("user-data-dir=" + self.my_profile)
        self.driver = webdriver.Chrome(chrome_options=self.options)
        self.driver.get('https://vk.com/feed')

class get_all_data():
    """ получаем инфу в постах """
    def __init__(self):
        pass

    def start_all(self, driver):
        self.driver = driver
        driver.refresh()
        self.wall_data = []
        self.posts = self.driver.find_elements_by_class_name('feed_row')
        return self.getting_wall_data(self.posts)

    def format_url(self, urle):
        """ Убираем лишнее в url """
        self.urle = urle
        self.start_position = self.urle.find('("')
        self.end_position = self.urle.find('")')
        return self.urle[self.start_position + 2:self.end_position]

    def get_text(self, post):
        """ текст из поста """
        self.post = post
        try:
            self.text = self.post.find_element_by_class_name('wall_post_text').text
        except:
            self.text = ''
        return self.text

    def get_image(self, post):
        """ картинка из поста """
        self.post = post
        try:
            self.url = []
            self.only_post = self.post.find_element_by_class_name('wall_text')  # only post no comments, support replies
            self.photo = self.only_post.find_elements_by_class_name('image_cover')
            for i in range(len(self.photo)):
                self.background_image_property = self.photo[i].value_of_css_property('background-image')
                self.url_add = self.format_url(self.background_image_property)
                self.url.append(str(self.url_add))
        except:
            self.url = []
        return self.url

    def getting_wall_data(self, posts):
        self.posts = posts
        for self.post in self.posts:
            try:
                self.url = []
                self.check_add = self.post.find_elements_by_class_name('wall_text_name_explain_promoted_post')
                if self.check_add == []:  # проверка на рекламу
                    self.id = (self.post.find_element_by_class_name('_post').get_attribute('data-post-id'))[1:]
                    self.url = self.get_image(self.post)
                    self.text = self.get_text(self.post)
                    self.wall_data.append({"image": self.url, "text": self.text, "id": self.id, "link": "https://vk.com/wall-{}".format(self.id)})
            except:
                pass
        return self.wall_data

class write_everything():
    """записываем в json файлы и добавляем в бд"""
    def __init__(self):
        pass

    def write_json(self, wall_data, i):
        """записываем в файл json"""
        self.i = i
        self.wall_data = wall_data
        with open("f%dJSON.json" % self.i, "w", encoding='utf8') as write_file:
            json.dump(self.wall_data, write_file, ensure_ascii=False, indent=2)
        write_file.close()


    def write_db(self, wall1, wall2, wall3):
        """Записываем в бд"""
        self.wall1 = wall1
        self.wall2 = wall2
        self.wall3 = wall3
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


open_vk = open_vk() # открытие вк
start2 = get_all_data() #получение постов

time_to_write1 = start2.start_all(open_vk.driver)
time.sleep(600) #ожидаем пока появятся новые посты
time_to_write2 = start2.start_all(open_vk.driver)
time.sleep(600) #ожидаем пока появятся новые посты
time_to_write3 = start2.start_all(open_vk.driver)



start3 = write_everything()  #запись в файл и бд
"""многопоточная запись в json"""
threading.Thread(target=start3.write_json, args=[time_to_write1, 1]).start()
threading.Thread(target=start3.write_json, args=[time_to_write2, 2]).start()
threading.Thread(target=start3.write_json, args=[time_to_write3, 3]).start()
"""запись в бд"""
start3.write_db(time_to_write1, time_to_write2, time_to_write3)


