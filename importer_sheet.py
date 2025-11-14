import os
import sys
import glob

sys.path.insert(0, os.getcwd())
import gsheets_sync


def resolve_sid():
    if len(sys.argv) >= 3 and sys.argv[1].upper() == 'SHEET_ID':
        return sys.argv[2]
    if len(sys.argv) >= 2:
        return sys.argv[1]
    sid = os.getenv('SHEET_ID')
    if sid:
        return sid
    raise SystemExit('SHEET_ID diperlukan')


def main():
    sid = resolve_sid()
    paths = sorted(glob.glob('new_data/*.csv'))
    if not paths:
        print('Tidak ada CSV di new_data')
        return
    for p in paths:
        tab = os.path.splitext(os.path.basename(p))[0]
        if tab.endswith('_processed'):
            tab = tab[:-10]
        gsheets_sync.sync_csv_to_sheet(p, sid, tab, replace=True)
    gsheets_sync.build_global_summary(sid, 'Summary')
    print(sid)


if __name__ == '__main__':
    main()
