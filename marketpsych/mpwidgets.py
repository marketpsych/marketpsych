from dataclasses import dataclass
from IPython.display import display
import datetime
import io
import ipywidgets as widgets
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np


ASSET_CLASSES = "CMPNY CMPNY_AMER CMPNY_APAC CMPNY_EMEA CMPNY_ESG CMPNY_GRP COM_AGR COM_ENM COU COU_ESG COU_MKT CRYPTO CUR".split()
CMPNY_CLASSES = "CMPNY CMPNY_AMER CMPNY_APAC CMPNY_EMEA CMPNY_ESG".split()
FREQUENCIES = "W365_UDAI WDAI_UDAI WDAI_UHOU W01M_U01M".split()
HIGH_FREQUENCIES = "WDAI_UHOU W01M_U01M".split()
NOT_RMAS = "id assetCode windowTimestamp dataType systemVersion ticker".split()
DEFAULT_FREQUENCY = 'WDAI_UDAI'
DEFAULT_ASSET_CLASS = 'COM_ENM'
DEFAULT_START = datetime.datetime(2020, 12, 1)
DEFAULT_END = datetime.datetime(2020, 12, 31)


class LoginWidgets:
    def __init__(self):
        """
        Widgets for setting credentials
        """
        self.key_widget = widgets.FileUpload(
            accept='', 
            description='Key File:',
            multiple=False
            )

        self.id_widget = widgets.Text(
            value='',
            placeholder='Type your User ID',
            description='User ID:',
            disabled=False
            )

        self.key_widget.observe(self._key_handler, names='value')

    def _key_handler(self, change):
        """
        If a new file is uploaded, get key attributes.
        """
        self._key = change.new
        self._key_name = list(self._key.keys())[0]
        self._id_suggest = self._key_name.split(".")[0]
        self._key_content = change.new[self._key_name]['content']
        # Creates new attribute in key to hold content in byte format
        self.key_widget.content = io.StringIO(change.new\
            [self._key_name]['content'].decode())
        # Updates ID widget with likely ID number from key name
        self.id_widget.value = self._id_suggest

    def display(self):
        """
        Display widgets.
        """
        widgets_ = widgets.HBox([
            self.key_widget,
            self.id_widget])
        display(widgets_)


class LoaderWidgets:
    def __init__(self):
        """
        Widgets for loading the data into notebook.
        """
        self.trial_check_widget = widgets.Checkbox(
            value=True,
            description='Trial',
            disabled=False
            )
        self.asset_class_widget = widgets.Dropdown(
            description='Asset class:',
            disabled=False,
            options=(ASSET_CLASSES),
            value=DEFAULT_ASSET_CLASS
            )
        self.frequency_widget = widgets.Dropdown(
            description='Frequency:',
            disabled=False,
            options=(FREQUENCIES),
            value=DEFAULT_FREQUENCY
            )
        self.start_date_widget = widgets.DatePicker(
            description='Start date:',
            disabled=False,
            value = DEFAULT_START
            )
        self.end_date_widget = widgets.DatePicker(
            description='End date:',
            disabled=False,
            value = DEFAULT_END
            )
        # Warns the user in case of certain choices
        self.warning_widget = widgets.Output()

        # Renew dates information if user changes them. It is used to avoid
        # dates being passed to sftp as datetime.date rather than datetime.datetime
        self.start_date_widget.observe(self._start_date_handler, names='value')
        self.end_date_widget.observe(self._end_date_handler, names='value')
        self.asset_class_widget.observe(self._memory_event_handler, names='value')
        self.frequency_widget.observe(self._memory_event_handler, names='value')

    def _start_date_handler(self, change):
        """Makes sure start_date is datetime.datetime"""
        d = self.start_date_widget.value
        self.start_date_widget.value = datetime.datetime(d.year, d.month, d.day)

    def _end_date_handler(self, change):
        """Makes sure end_date is datetime.datetime"""
        d = self.end_date_widget.value
        self.end_date_widget.value = datetime.datetime(d.year, d.month, d.day)

    def _memory_event_handler(self, change):
        self.warning_widget.clear_output()
        with self.warning_widget:
            if (self.asset_class_widget.value in CMPNY_CLASSES) or \
               (self.frequency_widget.value in HIGH_FREQUENCIES):
                print("WARNING: Extremely memory demanding!!!")

    def display(self):
        """
        Display widgets.
        """
        widgets_ = [self.trial_check_widget,
                    self.asset_class_widget, 
                    self.frequency_widget, 
                    self.start_date_widget, 
                    self.end_date_widget, 
                    self.warning_widget]
        for widget in widgets_:
            display(widget)


class SlicerWidgets(LoaderWidgets):
    def __init__(self, df):
        """
        Widgets for slicing the dataframe.
        """
        self.df_ = df
        # Combobox later only works if assetCode is str
        self.df_.assetCode = df.assetCode.astype(str)

        self.dataTypes_ = self.df_.dataType.unique().tolist()
        self.assets_ = self.df_.assetCode.unique().tolist()
        self.rmas_ = list(set(self.df_.columns) - set(NOT_RMAS))
        self.buzzes_ = [False] + sorted([col for col in self.rmas_ if 'buzz' in col.lower()])
        self.dates_ = self.df_.windowTimestamp.sort_values().unique()

        self.filtered_rma = None
        self.filtered_buzz = None

        self.dataType_widget = widgets.Dropdown(
            options=(sorted(self.dataTypes_)),
            description='Data Type:',
            disabled=False,
            value = 'News_Social' if 'News_Social' in self.dataTypes_ else self.dataTypes_[0]
            )

        self.rma_widget = widgets.Dropdown(options=(sorted(self.rmas_)),
            description='Analytics:',
            disabled=False,
            value = 'sentiment' if 'sentiment' in self.rmas_ else self.rmas_[0]
            )

        self.asset_widget = widgets.Combobox(
                options=(sorted(self.assets_)),
                description='Asset:',
                disabled=False,
                value = self.assets_[0],
                continuous_update=False
            )

        self.buzz_weight_widget = widgets.Dropdown(
            options=self.buzzes_,
            description='Weighted by:',
            disabled=False,
            value = False
            )

        self.rolling_widget = widgets.BoundedIntText(
                value=1,
                min=1,
                max=len(self.dates_),
                step=1,
                description='Roll. window:',
                disabled=False,
                continuous_update=True,
                orientation='horizontal',
                readout=True,
                readout_format='d'
            )

        self.minval_widget = widgets.BoundedIntText(
                value=1,
                min=1,
                max=len(self.dates_),
                step=1,
                description='Min. period:',
                disabled=False,
                continuous_update=True,
                orientation='horizontal',
                readout=True,
                readout_format='d'
            )
    
        self.output = widgets.Output()
        self.plot_output = widgets.Output()

        self.dataType_widget.observe(self._common_filtering, names='value')
        self.rma_widget.observe(self._common_filtering, names='value')
        self.asset_widget.observe(self._common_filtering, names='value')
        self.rolling_widget.observe(self._common_filtering, names='value')
        self.minval_widget.observe(self._common_filtering, names='value')
        self.buzz_weight_widget.observe(self._common_filtering, names='value')
        # Already shows a plot with default configs
        self._common_filtering(None)


    def display(self):
        """
        Display widgets.
        """
        input_widgets = widgets.HBox([
            self.dataType_widget, self.rma_widget, self.asset_widget])
        display(input_widgets)

        plot_widgets = widgets.HBox([
           self.buzz_weight_widget, self.rolling_widget, self.minval_widget])
        display(plot_widgets)

        tab = widgets.Tab([self.plot_output, self.output])
        tab.set_title(0, 'RMA plot')
        tab.set_title(1, 'RMA data')
        display(tab)


    def _common_filtering(self, change):
        self.output.clear_output()
        self.plot_output.clear_output()
        
        queried_df = self.df_[(self.df_.dataType == self.dataType_widget.value) & 
                            (self.df_.assetCode == self.asset_widget.value)]\
                            .set_index('windowTimestamp')

        self.filtered_rma = queried_df[[self.rma_widget.value]]

        with self.plot_output:
            roll = self.rolling_widget.value
            minval = self.minval_widget.value

            # Gets both RMA and buzz in the same time format
            agg_rma = self.filtered_rma.copy()
            agg_rma.index = pd.to_datetime(agg_rma.index).strftime("%Y-%m-%d")

            if self.buzz_weight_widget.value != False:
                # Gets buzz in correct format
                self.filtered_buzz = queried_df[[self.buzz_weight_widget.value]]
                agg_buzz = self.filtered_buzz.copy()
                agg_buzz.index = pd.to_datetime(agg_buzz.index).strftime("%Y-%m-%d")
                # Recomputes RMA
                agg_rma = np.multiply(agg_rma, agg_buzz).rolling(roll, min_periods=min(minval, roll)).sum()
                agg_rma = np.divide(agg_rma, agg_buzz.rolling(roll, min_periods=1).sum())
            else:
                agg_rma = agg_rma.rolling(roll, min_periods=min(minval, roll)).mean()

            #### Plot 
            fig, ax = plt.subplots(figsize=(14, 7))
            agg_rma.plot(ax=ax, c="blue", lw=2)
            plt.show()
        with self.output:
            display(agg_rma)






