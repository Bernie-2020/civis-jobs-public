from datetime import datetime
import collections
import pytz

# Commonly used datetime formats
YYYY_MM_DD = "%Y-%m-%d"
YYYY_MM_DD_HH_MM_SS = "%Y-%m-%d %H:%M:%S"
YYYYMMDD = "%Y%m%d"
YYYYMMDD_HHMMSS = "%Y%m%d_%H%M%S"

# Commonly used timezones
TZ_UTC = pytz.utc
TZ_EST = pytz.timezone("US/Eastern")
TZ_CST = pytz.timezone("US/Central")
TZ_PST = pytz.timezone("US/Pacific")

DateFmt = collections.namedtuple(
    "DateFmt",
    "yyyy_mm_dd yyyy_mm_dd_hh_mm_ss yyyymmdd yyyymmdd_hhmmss raw")


def get_datetime(tz):
    return datetime.now(tz)


def get_datetime_utc():
    return get_datetime(TZ_UTC)


def get_datetime_est():
    return get_datetime(TZ_EST)


def get_datetime_cst():
    return get_datetime(TZ_CST)


def get_datetime_pst():
    return get_datetime(TZ_PST)


def get_date_utc():
    return get_datetime_utc().date()


def get_date_est():
    return get_datetime_est().date()


def get_date_cst():
    return get_datetime_cst().date()


def get_date_pst():
    return get_datetime_pst().date()


def get_date_common_fmts(dt):
    return DateFmt(
        yyyy_mm_dd=dt.strftime(YYYY_MM_DD),
        yyyy_mm_dd_hh_mm_ss=dt.strftime(YYYY_MM_DD_HH_MM_SS),
        yyyymmdd=dt.strftime(YYYYMMDD),
        yyyymmdd_hhmmss=dt.strftime(YYYYMMDD_HHMMSS),
        raw=dt
    )


def main():
    print("Printing current UTC time in common formats...")
    datetime_utc = get_datetime_utc()

    print(datetime_utc.strftime(YYYY_MM_DD))
    print(datetime_utc.strftime(YYYYMMDD))
    print(datetime_utc.strftime(YYYY_MM_DD_HH_MM_SS))
    print(datetime_utc.strftime(YYYYMMDD_HHMMSS))

    print("Printing current EST time in common formats...")
    datetime_est = get_datetime_est()

    print(datetime_est.strftime(YYYY_MM_DD))
    print(datetime_est.strftime(YYYYMMDD))
    print(datetime_est.strftime(YYYY_MM_DD_HH_MM_SS))
    print(datetime_est.strftime(YYYYMMDD_HHMMSS))

    print("Printing current PST time in common formats...")
    datetime_pst = get_datetime_pst()

    print(datetime_pst.strftime(YYYY_MM_DD))
    print(datetime_pst.strftime(YYYYMMDD))
    print(datetime_pst.strftime(YYYY_MM_DD_HH_MM_SS))
    print(datetime_pst.strftime(YYYYMMDD_HHMMSS))


if __name__ == '__main__':
    main()
