import csv
import pickle
import luigi
from luigi.format import UTF8, TextFormat
import requests
import pandas as pd
from bs4 import BeautifulSoup



class GetPage(luigi.Task):

    brand = luigi.Parameter()
    model = luigi.Parameter()
    page  = luigi.IntParameter()

    def output(self):
        return luigi.LocalTarget(f"data/drom/pages/{self.brand}-{self.model}-p{self.page}.html")

    def run(self):
        url = f"https://auto.drom.ru/{self.brand}/{self.model}/page{self.page}/"
        r = requests.get(url)

        with self.output().open('w') as f:
            f.write(r.text)


class GetRawData(luigi.Task):

    brand = luigi.Parameter()
    model = luigi.Parameter()
    pages = luigi.IntParameter()

    def requires(self):
        return [GetPage(self.brand, self.model, page+1) for page in range(self.pages)]

    def output(self):
        return luigi.LocalTarget(f"data/drom/pickle/{self.brand}-{self.model}.pickle")

    def run(self):
        cars = []

        for _input in self.input():
            with _input.open('r') as f:
                soup = BeautifulSoup(f, 'html.parser')
            cars.extend([{
                    "ref": car['href'],
                    "title": car.find(attrs={'data-ftid': 'bull_title'}).text,
                    "props": [p.text for p in car.findAll(attrs={'data-ftid': 'bull_description-item'})],
                    "price": car.find(attrs={'data-ftid': 'bull_price'}).text.replace('\xa0', ''),
                    "location": car.find(attrs={'data-ftid': 'bull_location'}).text,
                    "date": car.find(attrs={'data-ftid': 'bull_date'}).text,
                } for car in soup.findAll(attrs={'data-ftid': 'bulls-list_bull'})]
            )
        with open(self.output().path, 'wb') as f:
            pickle.dump(cars, f)


class GetCSV(luigi.Task):

    brand = luigi.Parameter()
    model = luigi.Parameter()
    pages = luigi.IntParameter()

    def requires(self):
        return GetRawData(self.brand, self.model, self.pages)

    def output(self):
        return luigi.LocalTarget(f"data/drom/csv/{self.brand}-{self.model}.csv")

    def run(self):
        with open(self.input().path, 'rb') as f:
            cars = pickle.load(f)
        result = [self.parse_car(car) for car in cars]
        with open(self.output().path, 'w', newline='') as csvfile:
            fields = self.get_fields()
            writer = csv.DictWriter(csvfile, fieldnames=fields)
            writer.writeheader()
            for r in result:
                writer.writerow(r)

    def parse_car(self, car):
        _car = {}
        _car['ref'] = car['ref']
        _car['title'] = car['title']
        _car['price'] = car['price']
        for prop in car['props']:
            _prop = prop.replace(',', '').strip()
            if 'км' in _prop:
                _car['mileage'] = _prop
            elif _prop in ['бензин', 'дизель']:
                _car['fuel'] = _prop
            elif 'л.с.' in _prop:
                _car['power'] = _prop
            elif _prop in ['передний','4WD']:
                _car['transmission'] = _prop
            elif _prop in ['механика', 'АКПП', 'вариатор']:
                _car['gearbox'] = _prop
            else:
                print(f"Unknown property {_prop}")
        _car['date'] = car['date']
        _car['location'] = car['location']
        return _car

    def get_fields(self):
        return ['ref', 'title', 'price', 'mileage', 'fuel', 'power', 'transmission', 'gearbox', 'date', 'location']


class GetData(luigi.Task):

    brand = luigi.Parameter()
    model = luigi.Parameter()
    pages = luigi.IntParameter()

    def requires(self):
        return GetCSV(self.brand, self.model, self.pages)

    def output(self):
        return luigi.LocalTarget(f"data/drom/ftr/{self.brand}-{self.model}.ftr")

    def run(self):
        self.df = pd.read_csv(self.input().path, encoding='cp1251')
        self.proceed_dataframe()
        self.df.to_feather(self.output().path)

    def proceed_dataframe(self):
        self.df['year'] = self.df.title.apply(lambda x: int(x[-4:]))
        self.df['id'] = self.df.ref.apply(lambda x: x.split('/')[-1].split('.')[0])
        self.df['index'] = self.df.id
        self.df.set_index('index')


if __name__ == '__main__':
    luigi.run()
