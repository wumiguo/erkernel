# ER-Kernel

A generic kernel



## 引用

中国链接服务 => http://cnplinker.cnpeak.com/

百度学术 => https://xueshu.baidu.com/

德国比勒费尔德（Bielefeld)大学图书馆 => https://www.base-search.net/


## Common Data Issue and Hints

- when a wrong data source name configured but its underlying data folder isn't 'datasourcename.parq'

You will get a failure alert on view failed to be created


- when a column 'A' map to 'B' and you still use the 'A' column in relationShipSpec, that's a big problem

You will get a failure alert like 'cannot resolve `subject.A` given input columns[ subject.B ] '

