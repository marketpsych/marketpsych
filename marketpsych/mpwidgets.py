from dataclasses import dataclass
from IPython.display import display
import datetime
import io
import ipywidgets as widgets
import matplotlib.pyplot as plt
import pandas as pd


ASSET_CLASSES = "CMPNY CMPNY_AMER CMPNY_APAC CMPNY_EMEA CMPNY_ESG CMPNY_GRP COM_AGR COM_ENM COU COU_ESG COU_MKT CRYPTO CUR".split()
FREQUENCIES = "W365_UDAI WDAI_UDAI WDAI_UHOU W01M_U01M".split()
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
            description='Key file:',
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

        # Renew dates information if user changes them. It is used to avoid
        # dates being passed to sftp as datetime.date rather than datetime.datetime
        self.start_date_widget.observe(self._start_date_handler, names='value')
        self.end_date_widget.observe(self._end_date_handler, names='value')

    def _start_date_handler(self, change):
        """Makes sure start_date is datetime.datetime"""
        d = self.start_date_widget.value
        self.start_date_widget.value = datetime.datetime(d.year, d.month, d.day)

    def _end_date_handler(self, change):
        """Makes sure end_date is datetime.datetime"""
        d = self.end_date_widget.value
        self.end_date_widget.value = datetime.datetime(d.year, d.month, d.day)

    def display(self):
        """
        Display widgets.
        """
        widgets_ = [self.trial_check_widget,
                    self.asset_class_widget, 
                    self.frequency_widget, 
                    self.start_date_widget, 
                    self.end_date_widget]
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
        self.dates_ = self.df_.windowTimestamp.sort_values().unique()
        self.filtered_sr = None

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

        self.output = widgets.Output()
        self.plot_output = widgets.Output()

        self.dataType_widget.observe(self._event_handler, names='value')
        self.rma_widget.observe(self._event_handler, names='value')
        self.asset_widget.observe(self._event_handler, names='value')
        self.rolling_widget.observe(self._event_handler, names='value')
        self._common_filtering()

    def display(self):
        """
        Display widgets.
        """
        input_widgets = widgets.HBox([
            self.dataType_widget, self.rma_widget, self.asset_widget])
        display(input_widgets)

        plot_widgets = widgets.HBox([self.rolling_widget])
        display(plot_widgets)

        tab = widgets.Tab([self.plot_output, self.output])
        tab.set_title(0, 'RMA plot')
        tab.set_title(1, 'RMA data')
        display(tab)


    def _common_filtering(self):
        self.output.clear_output()
        self.plot_output.clear_output()
        
        self.filtered_sr = self.df_[(self.df_.dataType == self.dataType_widget.value) & 
                            (self.df_.assetCode == self.asset_widget.value)]\
                            .set_index('windowTimestamp')[[self.rma_widget.value]]
        
        with self.output:
            display(self.filtered_sr)
        with self.plot_output:
            temp = self.filtered_sr
            roll = self.rolling_widget.value
            temp.index = pd.to_datetime(temp.index).strftime("%Y-%m-%d")
            fig, ax = plt.subplots(figsize=(14, 7))
            temp.rolling(roll, min_periods=min(10, roll)).mean()\
                .plot(ax=ax, c="blue", lw=2)
            plt.show()

    def _event_handler(self, change):
        self._common_filtering()






