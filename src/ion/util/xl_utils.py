#!/usr/bin/env python

"""Parsing and handling of XLS files"""

__author__ = 'Michael Meisinger'

import csv
import StringIO
import os
try:
    import xlrd
    has_xl = True
except ImportError:
    print "No xlrd in buildout/path"
    has_xl = False


class XLSParser(object):
    """Class that transforms an XLS file into a dict of csv files (str)"""

    def extract_csvs(self, file_content):
        sheets = self.extract_worksheets(file_content)
        csv_docs = {}
        for sheet_name, sheet in sheets.iteritems():
            csv_doc = self.dumps_csv(sheet)
            csv_docs[sheet_name] = csv_doc.splitlines()
#            csv_doc = self.dumps_csv_list(sheet)
#            csv_docs[sheet_name] = csv_doc
        return csv_docs

    def extract_worksheets(self, file_content):
        book = xlrd.open_workbook(file_contents=file_content)
        sheets = {}
        formatter = lambda(t,v): self.format_excelval(book,t,v,False)

        for sheet_name in book.sheet_names():
            raw_sheet = book.sheet_by_name(sheet_name)
            data = []
            for row in range(raw_sheet.nrows):
                (types, values) = (raw_sheet.row_types(row), raw_sheet.row_values(row))
                data.append(map(formatter, zip(types, values)))
            sheets[sheet_name] = data
        return sheets

    def dumps_csv(self, sheet):
        stream = StringIO.StringIO()
        csvout = csv.writer(stream, delimiter=',', doublequote=False, escapechar='\\')
        csvout.writerows( map(self.utf8ize, sheet) )
        csv_doc = stream.getvalue()
        stream.close()
        return csv_doc

    def dumps_csv_list(self, sheet):
        cvs_lines = []
        for line in sheet:
            stream = StringIO.StringIO()
            csvout = csv.writer(stream, delimiter=',', doublequote=False, escapechar='\\')
            csvout.writerow(self.utf8ize(line))
            csv_doc = stream.getvalue()
            stream.close()
            cvs_lines.append(csv_doc)
        return cvs_lines

    def tupledate_to_isodate(self, tupledate):
        (y,m,d, hh,mm,ss) = tupledate
        nonzero = lambda n: n!=0
        date = "%04d-%02d-%02d"  % (y,m,d)    if filter(nonzero, (y,m,d))                else ''
        time = "T%02d:%02d:%02d" % (hh,mm,ss) if filter(nonzero, (hh,mm,ss)) or not date else ''
        return date+time

    def format_excelval(self, book, type, value, wanttupledate):
        if   type == 2: # TEXT
            if value == int(value): value = int(value)
        elif type == 3: # NUMBER
            datetuple = xlrd.xldate_as_tuple(value, book.datemode)
            value = datetuple if wanttupledate else self.tupledate_to_isodate(datetuple)
        elif type == 5: # ERROR
            value = xlrd.error_text_from_code[value]
        return value

    def utf8ize(self, l):
        return [unicode(s).encode("utf-8") if hasattr(s,'encode') else s for s in l]


def read_xls_rows(xl_filename, sheet=None, col_headers=True):
    """
    Reads given xls/xlsx file and parses/returns rows.

    Returns a list of rows if no sheet given and a dict of sheet names mapping to rows otherwise.
    Can parse column headers from first row if col_headers==True, and return row dict instead of list.
    Param sheet can be None for all sheets returned, a sheet name (str) or a 0-based sheet number (int)
    """
    if not has_xl:
        raise RuntimeError("Missing xlrd import")
    if not os.path.exists(xl_filename):
        raise RuntimeError("XLS file does not exist")

    result = None
    workbook = xlrd.open_workbook(xl_filename)

    if sheet is None:
        result = {}
        sheets = workbook.sheet_names()
        for name in sheets:
            xl_sheet = workbook.sheet_by_name(sheet)
            result[name] = read_xls_sheet(xl_sheet, col_headers=col_headers)
    elif isinstance(sheet, basestring):
        xl_sheet = workbook.sheet_by_name(sheet)
        result = read_xls_sheet(xl_sheet, col_headers=col_headers)
    else:
        sheets = workbook.sheet_names()
        xl_sheet = workbook.sheet_by_name(sheets[int(sheet)])
        result = read_xls_sheet(xl_sheet, col_headers=col_headers)

    return result


def read_xls_sheet(xl_sheet, col_headers=True):
    curcol, colmap = 0, {}   # Will hold a mapping of column num to name
    if col_headers:
        for colnum in xrange(xl_sheet.ncols):
            cell_type = xl_sheet.cell_type(curcol, colnum)
            cell_value = xl_sheet.cell_value(curcol, colnum)
            if 1 <= cell_type <=4 and str(cell_value).strip():
                colmap[colnum] = str(cell_value)
        curcol += 1

    res_rows = []
    for rownum in xrange(curcol, xl_sheet.nrows):
        data_entry = {} if col_headers else [None]*xl_sheet.ncols
        for colnum in xrange(xl_sheet.ncols):
            cell_type = xl_sheet.cell_type(rownum, colnum)
            cell_value = xl_sheet.cell_value(rownum, colnum)
            # Cell Types: 0=Empty, 1=Text, 2=Number, 3=Date, 4=Boolean, 5=Error, 6=Blank
            if col_headers:
                if colnum in colmap:
                    if cell_type == 1:
                        data_entry[colmap[colnum]] = unicode(cell_value).encode("utf8")
                    elif 2 <= cell_type <=4:
                        data_entry[colmap[colnum]] = cell_value
                    else:
                        data_entry[colmap[colnum]] = ""
            else:
                if cell_type == 1:
                    data_entry[colnum] = unicode(cell_value).encode("utf8")
                elif 2 <= cell_type <=4:
                    data_entry[colnum] = cell_value

        res_rows.append(data_entry)

    return res_rows