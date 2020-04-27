import pandas as pd
import numpy as np
import sys 

df = pd.read_csv(sys.argv[1])
gl_df = df.agg({'show': 'sum', 'click': 'sum'})
gl_df['ctr'] = gl_df['click'] / gl_df['show']
gl_df = pd.DataFrame(data={'key': ["global_ctr"], 'ctr': [gl_df['ctr'].item()]})

df = df.loc[((df.ctr.notnull()) & (df.show > 100) & (df.click > 1)) ]
tu_gb = df.groupby('tu', as_index=False).agg({'show': 'sum', 'click': 'sum'})
tu_gb['ctr'] = tu_gb['click'] / tu_gb['show']
tu_gb['key'] = tu_gb.apply(lambda x: int(x['tu']), axis=1)
tu_gb = tu_gb[['key', 'ctr']]

tu_adid_gb = df.groupby(['tu', 'adid'], as_index=False).agg({'show': 'sum', 'click': 'sum'})
tu_adid_gb['ctr'] = tu_adid_gb['click'] / tu_adid_gb['show']
tu_adid_gb['key'] = tu_adid_gb.apply(lambda x: "%s_%s" % (int(x['tu']), int(x['adid'])), axis=1)
tu_adid_gb = tu_adid_gb[['key', 'ctr']]

combine_df = pd.concat([gl_df, tu_gb, tu_adid_gb], axis=0)
combine_df['ctr_r'] = combine_df['ctr'].map(lambda x: round(x, 10))

combine_df.to_csv('stat_ctr.csv', columns=['key', 'ctr_r'], index=False, header=False)
