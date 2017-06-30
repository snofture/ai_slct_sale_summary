# -*- coding: utf-8 -*-
"""
Created on Thu Jun 29 15:38:06 2017

@author: limen
"""

import numpy as np
import pandas as pd
import os
import sys
import datetime

#os.chdir('C:/Users/limen/Documents/ai_slct_sale_summary/')


ords = pd.read_table('gdm_m04_ord_det_sum_trail.csv', sep='\t', encoding = 'utf-8')

#average price
sale = ords.groupby(['item_sku_id'])['before_prefr_amount'].sum()
qty = ords.groupby(['item_sku_id'])['sale_qtty'].sum()

price = sale/qty



# first day of sale
first_day_of_sale = ords.groupby(['item_sku_id'])['sale_ord_dt'].min().apply(lambda x: x[:10])


# time frame for average_net_sales and total_net_sales
end_date = datetime.datetime.strptime('2017-02-28','%Y-%m-%d')
year = end_date.year
month = end_date.month
start_month = '%04d-%02d' % (year - 1, month)
end_month = '%04d-%02d' % (year, month)


# avergae monthly net sales
ords['month'] = map(lambda x: x[:7], ords['sale_ord_tm'])
# monthly is the net_sales by item-sku_id and month
monthly = ords.groupby(['item_sku_id','month'])['after_prefr_amount'].agg({'net_sales':np.sum}).reset_index()
# monthly2 = ords.groupby(['item_sku_id','month'])['after_prefr_amount'].sum().reset_index()
# average net_sales by item_sku_id during the timeframe
average_net_sales = monthly[(monthly['month']>start_month) & 
                            (monthly['month']<=end_month)].groupby('item_sku_id')['net_sales'].mean()

# total net sales by item_sku_id during the timeframe
total_net_sales = monthly[(monthly['month']>start_month) & 
                          (monthly['month']<=end_month)].groupby('item_sku_id')['net_sales'].sum()



#merge data
sale_summary = pd.DataFrame(price, columns = ['price'])
sale_summary['first_day_of_sale'] = first_day_of_sale
sale_summary['average_net_sales'] = average_net_sales
sale_summary['total_net_sales'] = total_net_sales


#predict GMV of pops on self_owned platform
skus = pd.read_table('gdm_m03_item_sku_da.csv', sep = '\t', encoding = 'utf-8')
skus.set_index('item_sku_id', inplace = True)
sale_summary['sku_type'] = skus['sku_type']
# total means all skus' total_net_sales during the timeframe
total_self = sale_summary[sale_summary['sku_type'] == 'self']['total_net_sales'].sum()
total_pop = sale_summary[sale_summary['sku_type'] == 'pop']['total_net_sales'].sum() 
ratio = total_self/total_pop
ratios = sale_summary['sku_type'].apply(lambda x: int(x=='self') or ratio)
sale_summary['pop_predicted_sales'] = sale_summary['average_net_sales']*ratios

#predict GMV of tmalls on self owned platform
spus = pd.read_table('app_ai_slct_sku.csv',sep='\t',encoding='utf-8')
gmvs = pd.read_table('app_ai_slct_gmv.csv',sep='\t',encoding='utf-8')

spus['month'] = spus['dt'].apply(lambda x:x[:7])
gmvs['month'] = gmvs['dt'].apply(lambda x:x[:7])
spus = spus[(spus['web_id'] == 2) & (spus['month']>start_month) & (spus['month']<=end_month)]
gmvs = gmvs[(gmvs['web_id'] == 2) & (gmvs['month']>start_month) & (gmvs['month']<=end_month)]

spus = spus[['spu_id','sku_id','month']].drop_duplicates()
gmvs = gmvs[['spu_id','month','gmv','price']].drop_duplicates()

sku_name = spus.groupby(['spu_id','month'],as_index=False).agg({'sku_id':'count'}).rename(columns={'sku_id':'sku_count'})
gmvs = pd.merge(gmvs,sku_name,on=['spu_id','month'],how='inner')#to add the sku_count column
gmvs['sku_gmv'] = gmvs['gmv']/gmvs['sku_count']
gmvs = pd.merge(gmvs,spus,on= ['spu_id','month'],how='inner')#to add the sku_id column

total_tmall = gmvs['sku_gmv'].sum() or 1
ratio1 = total_self/total_tmall
tmall_sale_summary = gmvs.groupby(['sku_id']).agg({'sku_gmv':'mean','price':'mean'}).rename(columns={'sku_gmv':'average_net_sales'})
tmall_sale_summary['total_net_sales'] = gmvs.groupby('sku_id')['sku_gmv'].sum()
tmall_sale_summary['predicted_net_sales'] = tmall_sale_summary['average_net_sales']*ratio1
tmall_sale_summary['sku_type'] = 'tmall'
























