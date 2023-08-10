from pathlib import Path
from pandas import read_csv, concat
from re import compile
from datetime import datetime, timedelta
from json import loads, dumps
from defs.NSE import NSE
from defs.Dates import Dates
from os import SEEK_END, SEEK_CUR
from os.path import getmtime, getsize
from zipfile import ZipFile
from datetime import datetime, timedelta

DIR = Path(__file__).parent.parent
daily_folder = DIR / 'sme_data' / 'daily'
isin_file = DIR / 'sme_data' / 'isin.csv'
nseActionsFile = DIR / 'sme_data' / 'nse_actions.json'

isin = read_csv(isin_file, index_col='ISIN')

split_regex = compile('(\d+\.?\d*)[\/\- a-z\.]+(\d+\.?\d*)')

bonus_regex = compile('(\d+) ?: ?(\d+)')

dates = Dates()

has_latest_holidays = False


def getHolidayList(nse: NSE, file: Path):
    """Makes a request for NSE holiday list for the year.
    Saves and returns the holiday Object"""

    global has_latest_holidays

    url = 'https://www.nseindia.com/api/holiday-master'

    params = {'type': 'trading'}

    data = nse.makeRequest(url, params)

    # CM pertains to capital market or equity holdays
    data = {k['tradingDate']: k['description'] for k in data['CM']}

    file.write_text(dumps(data, indent=3))

    print('NSE Holiday list updated')

    has_latest_holidays = True
    return data


def isHolidaysFileUpdated(file: Path):
    """Returns True if the holiday.json files exists and
    year of download matches the current year"""

    return file.is_file() and datetime.fromtimestamp(getmtime(file)).year == dates.dt.year


def checkForHolidays(nse: NSE):
    """Returns True if current date is a holiday.
    Exits the script if today is a holiday"""

    file = DIR / 'sme_data' / 'holiday.json'

    if isHolidaysFileUpdated(file):
        # holidays are updated for current year
        holidays = loads(file.read_bytes())
    else:
        # new year get new holiday list
        holidays = getHolidayList(nse, file)

    # the current date for which data is being synced
    curDt = dates.dt.strftime('%d-%b-%Y')
    isToday = curDt == dates.today.strftime('%d-%b-%Y')

    if curDt in holidays:
        if not has_latest_holidays:
            holidays = getHolidayList(nse, file)

        if not isToday:
            print(f'{curDt} Market Holiday: {holidays[curDt]}')
            return True

        exit(f'Market Holiday: {holidays[curDt]}')

    return False


def validateNseActionsFile(nse: NSE):
    """Check if the NSE Corporate actions() file exists.
    If exists, check if the file is older than 7 days.
    Else request actions for the next 8 days from current date.
    The nse_actions.json pertains to Bonus, Splits, dividends etc."""

    if not nseActionsFile.is_file():
        getActions(nse, dates.dt, dates.dt + timedelta(8))
    else:
        lastModifiedTS = getmtime(nseActionsFile)

        # Update every 7 days from last download
        if dates.dt.timestamp() - lastModifiedTS > 7 * 24 * 60 * 60:
            frm_dt = datetime.fromtimestamp(lastModifiedTS) + timedelta(7)
            getActions(nse, frm_dt, dates.dt + timedelta(8))


def getActions(nse: NSE, from_dt: datetime, to_dt: datetime):
    """Make a request for corporate actions specifing the date range"""

    print('Updating NSE corporate actions file')
    fmt = '%d-%m-%Y'

    params = {
        'index': 'equities',
        'from_date': from_dt.strftime(fmt),
        'to_date': to_dt.strftime(fmt),
    }

    data = nse.makeRequest(
        'https://www.nseindia.com/api/corporates-corporateActions', params=params)

    nseActionsFile.write_text(dumps(data, indent=3))


def downloadNseBhav(nse: NSE, exitOnError=True):
    """Download the daily report for Equity bhav copy and
    return the saved file path. Exit if the download fails"""

    dt_str = dates.dt.strftime('%d%b%Y').upper()
    month = dt_str[2:5].upper()

    url = f'https://archives.nseindia.com/content/historical/EQUITIES/{dates.dt.year}/{month}/cm{dt_str}bhav.csv.zip'

    bhavFile = nse.download(url)

    if not bhavFile.is_file() or getsize(bhavFile) < 500:
        bhavFile.unlink()
        if exitOnError:
            exit('Download Failed: ' + bhavFile.name)
        else:
            raise FileNotFoundError()

    return bhavFile


def updateSmeEOD(bhavFile: Path):
    """Update all stocks with latest price data from bhav copy"""

    isin_updated = False

    # cm01FEB2023bhav.csv.zip
    with ZipFile(bhavFile) as zip:
        csvFile = bhavFile.name.replace('.zip', '')

        with zip.open(csvFile) as f:
            df = read_csv(f, index_col='ISIN')

    # save the csv file to the below folder.
    folder = DIR / 'nseBhav' / str(dates.dt.year)

    # Create it if not exists
    if not folder.is_dir():
        folder.mkdir(parents=True)

    df.to_csv(folder / csvFile)

    # filter the dataframe for stocks series EQ, BE and BZ
    # https://www.nseindia.com/market-data/legend-of-series
    df = df[(df['SERIES'] == 'SM') | (df['SERIES'] == 'ST')]

    # iterate over each row as a tuple
    for t in df.itertuples(name=None):
        idx, sym, _, O, H, L, C, _, _, V, *_ = t

        sym_file = daily_folder / f'{sym.lower()}.csv'

        # ISIN is a unique identifier for each stock symbol.
        # When a symbol name changes its ISIN remains the same
        # This allows for tracking changes in symbol names and
        # updating file names accordingly
        if not idx in isin.index:
            isin_updated = True
            isin.at[idx, 'SYMBOL'] = sym

        # if symbol name does not match the symbol name under its ISIN
        # we rename the files in daily and delivery folder
        if sym != isin.at[idx, 'SYMBOL']:
            isin_updated = True
            old = isin.at[idx, 'SYMBOL'].lower()

            new = sym.lower()

            isin.at[idx, 'SYMBOL'] = sym

            sym_file = daily_folder / f'{new}.csv'
            old_file = daily_folder / f'{old}.csv'

            try:
                old_file.rename(sym_file)
            except FileNotFoundError:
                print(
                    f'ERROR: Renaming daily/{old}.csv to {new}.csv. No such file.')

            print(f'Name Changed: {old} to {new}')

        updateSmeSymbol(sym_file, O, H, L, C, V)

    if isin_updated:
        isin.to_csv(isin_file)


def updateSmeSymbol(sym_file, o, h, l, c, v):
    'Appends EOD stock data to end of file'

    text = ''

    if not sym_file.is_file():
        text += 'Date,Open,High,Low,Close,Volume\n'

    text += f'{dates.pandas_dt},{o},{h},{l},{c},{v}\n'

    with sym_file.open('a') as f:
        f.write(text)


def adjustNseStocks():
    '''Iterates over NSE corporate actions searching for splits or bonus
    on current date and adjust the stock accordingly'''

    dt_str = dates.dt.strftime('%d-%b-%Y')

    actions = loads(nseActionsFile.read_bytes())

    # Store all Dataframes with associated files names to be saved to file
    # if no error occurs
    df_commits = []

    try:
        for act in actions:
            sym = act['symbol']
            purpose = act['subject'].lower()
            ex = act['exDate']
            series = act['series']

            if not series in ('EQ', 'BE', 'BZ'):
                continue

            if ('split' in purpose or 'splt' in purpose) and ex == dt_str:
                adjustmentFactor = getSplit(sym, purpose)

                if adjustmentFactor is None:
                    continue

                df_commits.append(makeAdjustment(sym, adjustmentFactor))

                print(f'{sym}: {purpose}')

            if 'bonus' in purpose and ex == dt_str:
                adjustmentFactor = getBonus(sym, purpose)

                if adjustmentFactor is None:
                    continue

                df_commits.append(makeAdjustment(sym, adjustmentFactor))

                print(f'{sym}: {purpose}')
    except Exception as e:
        # discard all Dataframes and raise error so changes can be rolled back
        df_commits = []
        raise e

    # commit changes
    for df, file in df_commits:
        df.to_csv(file)


def cleanup(files_lst):
    '''Remove files downloaded from nse and stock csv files not updated
    in the last 365 days'''

    for file in files_lst:
        file.unlink()

    # remove outdated files
    deadline = dates.today - timedelta(365)
    count = 0
    fmt = '%Y-%m-%d'

    for file in daily_folder.iterdir():
        lastUpdated = datetime.strptime(getLastDate(file), fmt)

        if lastUpdated < deadline:
            file.unlink()

            count += 1

    print(f'{count} files deleted')


def getSplit(sym, string):
    '''Run a regex search for splits related corporate action and
    return the adjustment factor'''

    match = split_regex.search(string)

    if match is None:
        print(f'{sym}: Not Matched. {string}')
        return match

    return float(match.group(1)) / float(match.group(2))


def getBonus(sym, string):
    '''Run a regex search for bonus related corporate action and
    return the adjustment factor'''

    match = bonus_regex.search(string)

    if match is None:
        print(f'{sym}: Not Matched. {string}')
        return match

    return 1 + int(match.group(1)) / int(match.group(2))


def makeAdjustment(symbol: str, adjustmentFactor: float):
    '''Makes adjustment to stock data prior to ex date,
    returning a tuple of pandas DataFrame and filename'''

    file = daily_folder / f'{symbol.lower()}.csv'

    if not file.is_file():
        print(f'{symbol}: File not found')
        return

    df = read_csv(file,
                  index_col='Date',
                  parse_dates=True,
                  na_filter=False)

    idx = df.index.get_loc(dates.dt)

    last = df.iloc[idx:]

    df = df.iloc[:idx].copy()

    for col in df.columns:
        if col == 'Volume':
            continue

        # nearest 0.05 = round(nu / 0.05) * 0.05
        df[col] = ((df[col] / adjustmentFactor / 0.05).round() * 0.05).round(2)

    df = concat([df, last])
    return (df, file)


def getLastDate(file):
    'Get the last updated date for a stock csv file'

    # source: https://stackoverflow.com/a/68413780
    with open(file, 'rb') as f:
        try:
            # seek 2 bytes to the last line ending ( \n )
            f.seek(-2, SEEK_END)

            # seek backwards 2 bytes till the next line ending
            while f.read(1) != b'\n':
                f.seek(-2, SEEK_CUR)

        except OSError:
            # catch OSError in case of a one line file
            f.seek(0)

        # we have the last line
        lastLine = f.readline().decode()

    # split the line (Date, O, H, L, C) and get the first item (Date)
    return lastLine.split(',')[0]


def rollback(folder: Path):
    '''Iterate over all files in folder and delete any lines
    pertaining to the current date'''

    dt = dates.pandas_dt
    print(f"Rolling back changes from {dt}: {folder}")

    for file in folder.iterdir():
        df = read_csv(file, index_col='Date',
                      parse_dates=True, na_filter=False)

        if dt in df.index:
            df = df.drop(dt)
            df.to_csv(file)

    print('Rollback successful')
