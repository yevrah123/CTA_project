from abc import ABCMeta, abstractmethod
import pandas as pd
import numpy as np
import datetime


class Worker(metaclass=ABCMeta):

    def __init__(self):
        pass

    @abstractmethod
    def run(self, *args, **kwargs):
        pass


class TimerCleaner(Worker):

    def __init__(self):
        super().__init__()

        self.initial_form()

    def run(self, df):

        self.initial_form()

        number_of_datas = len(df)

        df = self.pre_clean(df)

        night_df, day_df = self.separate(df)

        if not night_df.empty:
            date = night_df.iloc[0]['date']

            night_schedule = self.set_schedule(date=date, day=False)

            night_df = self.check(night_df, night_schedule)

        if not day_df.empty:
            date = day_df.iloc[0]['date']

            day_schedule = self.set_schedule(date=date, day=True)

            day_df = self.check(day_df, day_schedule)

        df = pd.concat([night_df, day_df], axis=0)

        return df

    def pre_clean(self, df):

        df.drop("_id", inplace=True, axis=1)

        df.drop_duplicates(inplace=True)

        df['next_datetime'] = df['datetime'].shift(-1)

        df.drop(
            list(df[df['datetime'] == df['next_datetime']].index), inplace=True)

        df.set_index('datetime', inplace=True, drop=True)

        df['attribute'] = 1

        return df

    def initial_form(self):

        self.form = {}

    def separate(self, df):

        night_df = df[('20:30:00.0' <= df['time']) & (
                df['time'] <= '23:30:00.0')]

        day_df = df[('08:30:00.0' <= df['time']) & (
                df['time'] <= '15:30:00.0')]

        return night_df, day_df

    def set_schedule(self, date, day=False):

        if not day:

            schedule = pd.date_range(start=date + ' 21:00:00', end=date + ' 23:00:00', freq='500L')

        else:

            day_1 = pd.date_range(start=date + ' 9:00:00', end=date + ' 10:15:00', freq='500L')

            day_2 = pd.date_range(start=date + ' 10:30:00', end=date + ' 11:30:00', freq='500L')

            day_3 = pd.date_range(start=date + ' 13:30:00', end=date + ' 15:00:00', freq='500L')

            schedule = day_1.append([day_2, day_3])

        schedule = pd.DataFrame(index=schedule)

        return schedule

    def check(self, df, schedule):

        df = pd.concat(
            [schedule, df], axis=1, sort=True)
        df['attribute'].replace(np.nan, 0, inplace=True)
        df.fillna(method='ffill', inplace=True)
        df.drop(['dummy_date', 'next_time'], axis=1)

        return df


class FieldCleaner(Worker):

    def run(self, df):
        pass


class Weighter(Worker):

    def run(self, df):
        pass
