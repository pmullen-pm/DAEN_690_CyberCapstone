import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import warnings


if __name__ == '__main__':
    #Set package and environment vars for notebook
    warnings.filterwarnings('ignore')
    #%matplotlib inline
    plt.style.use('ggplot')
    plt.rcParams['xtick.labelsize'] = 15
    plt.rcParams['ytick.labelsize'] = 15
    #Load data sets
    sectors = pd.read_csv(filepath_or_buffer='sectors.csv', names=['sector','count'])
    time = pd.read_csv(filepath_or_buffer='time.csv', names=['first_seen', 'count'])
    time['first_seen'] = pd.to_datetime(arg=time['first_seen'], unit='s')
    scores = pd.read_csv(filepath_or_buffer='scores.csv', names=['dark3_score', 'count'])
    locations = pd.read_csv(filepath_or_buffer='locations.csv', names=['country', 'dark3_score', 'count'])
    locations_reduced = pd.read_csv(filepath_or_buffer='location_scores_reduced.csv', names=['country', 'dark3_score', 'count'])
    score_direction = pd.read_csv(filepath_or_buffer='score_direction.csv', names=['label', 'count'])
    dt = pd.read_csv(filepath_or_buffer='dt.csv', names=['dt', 'count'])
    dt['dt'] = dt['dt']/1000
    
    sectors.loc[sectors['sector'] == 'IT & Svcs', 'sector'] = 'Information Technology & Services' 
    sectors.loc[sectors['sector'] == 'healthcare', 'sector'] = 'Health Care' 
    sectors.loc[sectors['sector'] == 'govt services', 'sector'] = 'Government' 
    sectors.loc[sectors['sector'] == 'government', 'sector'] = 'Government' 
    sectors.loc[sectors['sector'] == 'IT Sector', 'sector'] = 'Information Technology & Services' 
    sectors.loc[sectors['sector'] == 'IT', 'sector'] = 'Information Technology & Services' 
    sectors.loc[sectors['sector'] == 'cybersecurity', 'sector'] = 'Cybersecurity' 
    sectors.loc[sectors['sector'] == 'home', 'sector'] = 'Home'
    sectors.loc[sectors['sector'] == 'infrastructure', 'sector'] = 'Infrastructure'
    sectors.loc[sectors['count'] <=300000, 'sector'] = 'Other'
    sectors = sectors.groupby(['sector'])['count'].sum()
    
    ax = sectors.sort_values(ascending=False).plot(x='sector', y='count', title='Frequency of Records by Sector', kind='bar', figsize=(20, 10))
    ax.set_xlabel('Sector', fontsize=15)
    ax.set_ylabel('Frequency (10 Million)', fontsize=15)
    
    ax = scores.dropna().sort_values(by='count', ascending=False).plot(x='dark3_score', y='count', title='Frequency of Records by Score', kind='bar', figsize=(20, 10))
    ax.set_xlabel('Dark3 Score', fontsize=15)
    ax.set_ylabel('Frequency (100 Million)', fontsize=15)
    
    ax = locations_reduced.pivot(index='country', columns='dark3_score', values='count').sort_values(by=['High','Neutral','Low'], ascending=False).head(10).plot(title='Top 10 Countries by Frequency', kind='bar', figsize=(20, 10))
    ax.set_xlabel('Country', fontsize=15)
    ax.set_ylabel('Frequency (10 Million)', fontsize=15)
    
    ts = pd.Series(data=time['count'].as_matrix(), index=time['first_seen'])
    ts.describe()
    ax = ts['2016':'2017'].plot(title='Traffic First Seen', figsize=(20, 10))
    ax.set_xlabel('Date', fontsize=15)
    ax.set_ylabel('Count Seen', fontsize=15)
    ax = ts['2016':'2017'].ewm(alpha=0.2).mean().plot(title='Moving Average Series', figsize=(20, 10))
    ax.set_xlabel('Date', fontsize=15)
    ax.set_ylabel('Average', fontsize=15)
    base = ts['2016':'2017']
    emw = ts['2016':'2017'].ewm(alpha=0.2).mean()
    combined = [base,emw]
    result = pd.concat(combined, axis=1, keys=['count_seen', 'moving_average'])
    ax = result.plot(title='Time Series Analysis', figsize=(20, 10))
    ax.set_xlabel('Date', fontsize=15)
    ax.set_ylabel('Frequency', fontsize=15)
    ax = score_direction.dropna().sort_values(by=['count'], ascending=False).plot(x='label', y='count', title='Frequency of Records by Score Grouping', kind='bar', figsize=(20, 10))
    ax.set_xlabel('Grouping Label', fontsize=15)
    ax.set_ylabel('Frequency (10 Million)', fontsize=15)
    
    total = ts.sum()
    stop = total * 0.75
    cumSum = 0
    ts.sort_index()
    for i in ts.iteritems():
        cumSum = cumSum + i[1]
        if cumSum >= stop:
            print i, cumSum
            break
        
    ax = dt.plot(x='dt', y='count', title='Time Lapse', kind='scatter', figsize=(20, 10))
    ax.set_xlabel('Time (seconds)', fontsize=15)
    ax.set_ylabel('Frequency (10 Million)', fontsize=15)