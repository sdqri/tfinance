tfinance - Tehran Stock Exchange OSINT Tool for Python
=======================================================

.. badges:

.. image:: https://img.shields.io/badge/python-2.7,%203.4+-blue.svg?style=flat
    :target: https://pypi.python.org/pypi/tfinance
    :alt: Python version

.. image:: https://img.shields.io/pypi/v/tfinance.svg?maxAge=60
    :target: https://pypi.python.org/pypi/tfinance
    :alt: PyPi version

.. image:: https://img.shields.io/pypi/dm/tfinance.svg?maxAge=2592000&label=installs&color=%2327B1FF
    :target: https://pypi.python.org/pypi/tfinance
    :alt: PyPi downloads




This is just an Alpha release, So If you find it interesting or you encounter any bugs, I would appreciate if you let me know! 

Overview
--------

Currently there is no active well-documented API for TSE capital market out there. And this is a big obstacle to development of projects and studies based on this data. **tfinance** aimes to solve this problem by developing an OSINT tool to provide a reliable, threaded, and Pythonic way to access TSE's historical data.

.. code:: python

    >> import tfinance as tfin
    >> fold = tfin.TSE("فولاد")
    >>fold.name
    'فولاد مباركه اصفهان'
    >>fold.sector
    'فلزات اساسي'
    >>fold.history.head()
                <TICKER> <DTYYYYMMDD>  <FIRST>  ...  <PER>   <OPEN>   <LAST>
    0  S*Mobarakeh.Steel   2020-10-06  17700.0  ...      D  17860.0  18240.0
    1  S*Mobarakeh.Steel   2020-10-05  17610.0  ...      D  18210.0  17700.0
    2  S*Mobarakeh.Steel   2020-10-04  18760.0  ...      D  18710.0  18090.0
    3  S*Mobarakeh.Steel   2020-10-03  18290.0  ...      D  17920.0  18810.0
    4  S*Mobarakeh.Steel   2020-09-30  18170.0  ...      D  17500.0  18290.0
    [5 rows x 12 columns]


Installation
------------

The easiest way to install ``tfinance`` is from the `Python Package Index <https://pypi.org/project/tfinance/>`_
using ``pip`` or ``easy_install``:

.. code-block:: bash

    $ pip install tfinance

.. Documentation
.. -------------


License
-------

.. image:: https://img.shields.io/pypi/l/tfinance?color=green