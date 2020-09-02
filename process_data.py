"""
Function: test functions of reading mf4 files using the corresponding dbc and extract the wanted signals from excel
Author: Yiming Gu, Xinran Wang
Date: 09/02/2020
"""

import asammdf
from asammdf import MDF
import glob
import pandas as pd
import os
import time
import sys

from pyparsing import Word, Literal, Keyword, Optional, Suppress, Group, QuotedString, Combine
from pyparsing import printables, nums, alphas, alphanums, LineEnd, ZeroOrMore, OneOrMore

sys.setrecursionlimit(100000)
pd.set_option('expand_frame_repr', False)

# DBC section types
VERSION = 'VERSION'
ECU = 'BU_'
COMMENT = 'CM_'
MESSAGE = 'BO_'
SIGNAL = 'SG_'
VALTYPE = 'SIG_VALTYPE_'
VALUETABLE = 'VAL_'
BA = 'BA_'
BADEF = 'BA_DEF_'
BADEFDEF = 'BA_DEF_DEF_'
BADEFREF = 'BA_DEF_REF_'

# def load_dbc(dbc_file_dir):
#     dbc = glob.glob(dbc_file_dir + "FR*.dbc")
#     return dbc


class Signal:
    """
    CAN Signal object
    Attributes:
        name: signal name, as string
        multi_type: normal signal or multiplexor/multiplexed signal ('N' for normal signal, 'M' for multiplexor, number for multiplexed signal)
        start_bit: start bit of the signal, as uint, original from dbc
        length_bit: the bit length of the signal, as uint
        byte_order: little endien (1: 'Intel') or big endien (0: 'Motorola'), as boolean
        value_type: unsigned (0) or signed (1), as boolean
        factor: for float value, as float
        offset: for float value, as float
        unit: unit, as string
        min: minimum value of the signal
        max: maximum value of the signal
        value_table: value table of the signal
        comment: comment
    Methods: None
    """

    def __init__(self, name, multi_type, start_bit, length_bit, byte_order, value_type, factor, offset, unit, value_min,
                 value_max, value_table, comment):
        self.name = name
        self.multi_type = multi_type
        self.start_bit = start_bit
        self.length_bit = length_bit
        self.byte_order = byte_order
        self.value_type = value_type
        self.factor = factor
        self.offset = offset
        self.value_min = value_min
        self.value_max = value_max
        self.unit = unit
        self.value_table = value_table
        self.comment = comment

    # def __repr__(self):
    #     fmt = 'signal(' + ', '.join(12 * ['{}']) + ')'
    #     return fmt.format(self.name,
    #                       self.start,
    #                       self.length,
    #                       self.byte_order,
    #                       self.type,
    #                       self.scale,
    #                       self.offset,
    #                       self.min,
    #                       self.max,
    #                       self.unit,
    #                       self.choices,
    #                       self.comment)


class Message:
    """
    CAN Message object
    Attributes:
        id_hex: frame id in hex, as string, also the keyword of a frame
        id_dec: frame id in dec, as unsigned int
        name: frame name, as string
        dlc: frame dlc, as uint
        type: frame type, as uint (0: CAN standard, 1: ISO CAN FD, 2: Non ISO CAN FD)
        transmitter: frame transmitter, as string
        receiver: frame receiver, as string
        cycle_time: frame cycle time, as uint, in millisecond
        comment: frame comment, as string
        signals: signals dict for this frame, signal name as keyword in dict
    Methods:

    """

    def __init__(self, id_dec, name, dlc, cycle_time=0, message_type=None, transmitter=None, signals=None,
                 comment=None):
        self.id_dec = id_dec
        self.id_hex = hex(id_dec)
        self.name = name
        self.dlc = dlc
        self.cycle_time = cycle_time
        self.message_type = message_type
        self.transmitter = transmitter
        self.signals = signals
        self.comment = comment


class DBCFile:
    """CAN database file.
    """

    def __init__(self, messages=None):
        self.messages = messages if messages else []
        # self.grammar = self.create_dbc_grammar()
        self.msg_id_dec = {}
        self.msg_id_hex = {}
        self.version = None
        self.ecus = None

    def read_dbcfile(self, dbc_file_content):
        """
        parser DBC file, create DBC file object including messages / signals information.
        """
        tokens = self.create_dbc_grammar().parseString(dbc_file_content)

        msg_comments = {}
        sig_comments = {}
        valtypes = {}
        value_tables = {}
        for item in tokens:
            if item[0] == COMMENT:
                frame_id = int(item[2])
                if item[1] == MESSAGE:
                    if frame_id not in msg_comments.keys():
                        msg_comments[frame_id] = item[3]
                elif item[1] == SIGNAL:
                    if frame_id not in sig_comments.keys():
                        sig_comments[frame_id] = {}
                    sig_comments[frame_id][item[3]] = item[4]
            elif item[0] == VALTYPE:
                frame_id = int(item[1])
                if frame_id not in valtypes:
                    valtypes[frame_id] = {}
                valtypes[frame_id][item[2]] = int(item[3])
            elif item[0] == VALUETABLE:
                frame_id = int(item[1])
                if frame_id not in value_tables.keys():
                    value_tables[frame_id] = {}
                value_tables[frame_id][item[2]] = [(int(v[0]), v[1]) for v in item[3]]
            elif item[0] == VERSION:
                self.version = item[1]
            elif item[0] == ECU:
                if len(item) > 1:
                    # print(item[1])
                    self.ecus = item[1]
            else:
                pass
        for item in tokens:
            if item[0] == MESSAGE and item[1] != '3221225472':
                message = Message(int(item[1]), item[2], int(item[3]), transmitter=item[4])
                if item[1] in msg_comments.keys():
                    message.comment = msg_comments[item[1]]
                message.signals = []
                for signal in item[5]:
                    if signal[2] == 'M':
                        message.signals.append(Signal(
                            name=signal[1],
                            multi_type='M',
                            start_bit=int(signal[3][0]),
                            length_bit=int(signal[3][1]),
                            byte_order=(0 if signal[3][2] == '0' else 1),
                            value_type=(0 if signal[3][3] == '+' else 1),
                            factor=num(signal[4][0]),
                            offset=num(signal[4][1]),
                            value_min=num(signal[5][0]),
                            value_max=num(signal[5][1]),
                            unit=signal[6],
                            value_table=None,
                            comment=None
                        ))
                    elif signal[2][0] == 'm':
                        message.signals.append(Signal(
                            name=signal[1],
                            multi_type=int(signal[2][1]),
                            start_bit=int(signal[3][0]),
                            length_bit=int(signal[3][1]),
                            byte_order=(0 if signal[3][2] == '0' else 1),
                            value_type=(0 if signal[3][3] == '+' else 1),
                            factor=num(signal[4][0]),
                            offset=num(signal[4][1]),
                            value_min=num(signal[5][0]),
                            value_max=num(signal[5][1]),
                            unit=signal[6],
                            value_table=None,
                            comment=None
                        ))
                    else:
                        message.signals.append(Signal(
                            name=signal[1],
                            multi_type='N',
                            start_bit=int(signal[2][0]),
                            length_bit=int(signal[2][1]),
                            byte_order=(0 if signal[2][2] == '0' else 1),
                            value_type=(0 if signal[2][3] == '+' else 1),
                            factor=num(signal[3][0]),
                            offset=num(signal[3][1]),
                            value_min=num(signal[4][0]),
                            value_max=num(signal[4][1]),
                            unit=signal[5],
                            value_table=None,
                            comment=None
                        ))
                for sig in message.signals:
                    # if message.id_dec in valtypes.keys():
                    #     if sig.name in valtypes[message.id_dec]:
                    #         sig.value_type = valtypes[message.id_dec][sig.name]
                    if message.id_dec in value_tables.keys():
                        if sig.name in value_tables[message.id_dec]:
                            sig.value_table = value_tables[message.id_dec][sig.name]
                    if message.id_dec in sig_comments.keys():
                        if sig.name in sig_comments[message.id_dec]:
                            sig.comment = sig_comments[message.id_dec][sig.name]
                self.add_message(message)

    def create_dbc_grammar(self):
        """Create DBC grammar.
        """

        # DBC file grammar
        word = Word(printables, excludeChars=':')
        integer = Combine(Optional(Literal('-')) + Word(nums))
        number = Word(nums + '.Ee-+')
        colon = Suppress(Literal(':'))
        scolon = Suppress(Literal(';'))
        pipe = Suppress(Literal('|'))
        at = Suppress(Literal('@'))
        sign = Literal('+') | Literal('-')
        lp = Suppress(Literal('('))
        rp = Suppress(Literal(')'))
        lb = Suppress(Literal('['))
        rb = Suppress(Literal(']'))
        comma = Suppress(Literal(','))
        multiplexor = Literal('M')
        multiplexed = Group(Literal('m') + number)

        version = Group(Keyword('VERSION') + QuotedString('"', multiline=True))
        symbol = Word(alphas + '_') + Suppress(LineEnd())
        symbols = Group(Keyword('NS_') + colon + Group(ZeroOrMore(symbol)))
        discard = Suppress(Keyword('BS_') + colon)
        ecu = Group(Keyword('BU_') + colon + ZeroOrMore(Word(printables).setWhitespaceChars(' \t')))
        signal = Group(Keyword(SIGNAL) + word + ZeroOrMore(multiplexor | multiplexed) + colon +
                       Group(integer + pipe + integer + at + integer + sign) +
                       Group(lp + number + comma + number + rp) +
                       Group(lb + number + pipe + number + rb) +
                       QuotedString('"', multiline=True) + word)
        message = Group(Keyword(MESSAGE) + integer + word + colon + integer + word + Group(ZeroOrMore(signal)))
        comment = Group(Keyword(COMMENT) + (
                (Keyword(MESSAGE) + integer + QuotedString('"', multiline=True) + scolon) |
                (Keyword(SIGNAL) + integer + word + QuotedString('"', multiline=True) + scolon)))
        badef = Group(Keyword(BADEF) + Optional(Keyword('BU_') | Keyword('BO_') | Keyword('SG_') | Keyword('EV_')) +
                      QuotedString('"') + (((Keyword('INT') | Keyword('HEX') | Keyword(
            'FLOAT')) + integer + integer + scolon) | (Keyword('ENUM') +
                                                       OneOrMore(QuotedString('"') + Optional(comma)) + scolon) | (
                                                       Keyword('STRING') + scolon)))
        badefdef = Group(Keyword(BADEFDEF) + QuotedString('"') + (QuotedString('"') | integer) + scolon)
        badefref = Group(Keyword(BADEFREF) + QuotedString('"') + (QuotedString('"') | integer) + scolon)
        ba = Group(Keyword(BA) + QuotedString('"') + Optional(
            Keyword('BU_') | Keyword('BO_') | Keyword('SG_') | Keyword('EV_')) + (
                               QuotedString('"') | OneOrMore(Word(alphanums))) + scolon)

        valtable = Group(Keyword('VAL_TABLE_') + Word(alphanums + '_') + Group(
            OneOrMore(Group(integer + QuotedString('"', multiline=True)))) + scolon)

        valtype = Group(Keyword(VALTYPE) + integer + word + colon + integer + scolon)

        choice = Group(Keyword(VALUETABLE) + integer + word + Group(
            OneOrMore(Group(integer + QuotedString('"', multiline=True)))) + scolon)

        entry = version | symbols | discard | ecu | message | comment | ba | badef | badefdef | badefref | valtable | valtype | choice
        grammar = OneOrMore(entry)

        return grammar

    def add_message(self, message):
        self.messages.append(message)
        self.msg_id_dec[message.id_dec] = message
        self.msg_id_hex[message.id_hex] = message

    def decode_message(self, frame_id, data):
        """Decode a message
        """

        message = self.frame_id_to_message[frame_id]
        return message.decode(data)


def load_dbc(file_path):
    """
    Load the dbc file from the given directory
    :param file_path: the path of directory storing all dbc files
    :return: a dictionary containing info extracted from DBC
    """
    dbc = DBCFile()
    with open(file_path, 'r', encoding='utf8', errors='replace') as f:
        dbc.read_dbcfile(f.read())
    f.close()

    msg_dict = {message.id_dec: {
                'id_dec': message.id_dec,
                'id_hex': message.id_hex,
                'name': message.name,
                'dlc': message.dlc,
                'comment': message.comment,
                'signals': {signal.name: {'name': signal.name,
                                            'multi_type': signal.multi_type,
                                            'start_bit': signal.start_bit,
                                            'length_bit': signal.length_bit,
                                            'byte_order':  signal.byte_order,
                                            'value_type': signal.value_type,
                                            'factor': signal.factor,
                                            'offset': signal.offset,
                                            'value_min': signal.value_min,
                                            'value_max': signal.value_max,
                                            'unit': signal.unit,
                                            'value_table': signal.value_table,
                                            'comment': signal.comment}
                            for signal in message.signals}}
                for message in dbc.messages if message.name != 'VECTOR__INDEPENDENT_SIG_MSG'}
    return msg_dict


def num(s):
    """
    convert a string to integer or float
    :param s: a string of number
    :return: an int or float type number
    """
    try:
        return int(s)
    except ValueError:
        return float(s)
    else:
        raise ValueError('Expected integer or floating point number.')


# def read_mf4(file_path, dbc):
#     mdf = MDF(file_path, "r")
#     print(mdf)
#     information = mdf.extract_can_logging(dbc)
#     data = information.to_dataframe()
#     return data


def extract_wanted_signal_data(dataframe, signal_excel_path):
    signals = pd.read_excel(signal_excel_path)
    names = list(signals["Name"])
    return dataframe.reindex(columns=names)


def load_total_matrix(root_path, dbc_channel_files):
    total_messages, total_signals, total_fullpath = {}, {}, {}
    for channel_key, file_list in dbc_channel_files.items():
        total_messages[channel_key] = {}
        total_signals[channel_key] = {}
        total_fullpath[channel_key] = []
        if len(file_list) == 0:
            print("No DBC for channel: " + channel_key[-1])
        else:
            for file in file_list:
                full_path = os.path.join(root_path, file)
                if os.path.exists(full_path):
                    if os.path.splitext(full_path)[-1] != ".dbc":
                        print("File is not DBC file: " + full_path)
                    else:
                        total_messages[channel_key].update(load_dbc(full_path))
                        total_fullpath[channel_key].append(full_path)
                else:
                    print("No such DBC file: " + full_path)
        for msg in total_messages[channel_key]:
            for sig in total_messages[channel_key][msg]["signals"]:
                total_signals[channel_key][sig] = total_messages[channel_key][msg]["signals"][sig]
    return total_fullpath, total_messages, total_signals


def loadMF4data2Dict(file, wanted_signals, dbcfiles=None):
    """
    Use the given signals, extract the wanted data from the data file
    :param file: the path of mf4 file
    :param wanted_signals: a list containing wanted signals
    :param dbcfiles: the total_fullpath generated from load_total_matrix
    :return: a dictionary
    """
    if not os.path.exists(file):
        print("Data file not found.")
        return None
    t0 = time.time()
    try:
        mdffile = asammdf.MDF(file, 'r')

        # signalList = list(mdffile.channels_db.keys())
        # print(signalList)
        # count = 0
        # total_sig_count = sum([len(totalSignals[channel_key]) for channel_key in totalSignals])
        # for sig in signalList:
        #     flag = 0
        #     if 'CAN_' in sig or 'Vector' in sig or 'time' == sig:
        #         continue
        #     else:
        #         for channel_key in totalSignals:
        #             if sig in totalSignals[channel_key]:
        #                 flag = 1
        #         if flag == 1:
        #             count += 1

        data = {}
        for channel_key in dbcfiles:
            channel_num = int(channel_key.split('Ch')[-1])
            if channel_num in mdffile.bus_logging_map['CAN']:
                channel_index = list(mdffile.bus_logging_map['CAN'][channel_num].values())[0]
                mdffile_ext = asammdf.MDF(file, 'r').filter([(None, channel_index, 1)]).extract_can_logging(dbcfiles[channel_key])
                for w in wanted_signals:
                    try:
                        if (w not in data) or (data[w] is None):
                            tmpdata = mdffile_ext.get(w)
                            data[w] = pd.DataFrame(tmpdata.samples, index=tmpdata.timestamps, columns=[w])
                    except:
                        data[w] = None
    except Exception as e:
        print(file + ': ' + str(e))
        return {}

    if len(data.keys()) == 0:
        print('No valid signal in file: ' + os.path.split(file)[-1])
    print('Loaded: ' + os.path.split(file)[-1] + ', time elapsed: ' + str(time.time() - t0) + 's')
    return data



if __name__ == "__main__":
    # path = "C:\\Users\\Z0050908\\Documents\\Reinj_data\\GWM_V71_39_02A01_RB_test_2020_08_26_070508#k826070508q1a24a7\\GWM_V71_39_02A01_RB_test_2020_08_26_070508_log_015.mf4"
    # # path = "C:\\Users\\Z0050908\\Documents\\Reinj_data\\Raw data\\GWM_TimeSycn_142_2020_07_11_070917_log_007.mf4"
    #
    # dbc_path = "C:\\Users\\Z0050908\\Desktop\\read_can_dbc"
    # # dbc_path = "C:\\Users\Z0050908\\Desktop\\read_can_dbc"
    # signal = "C:\\Users\\Z0050908\\Desktop\\FR-IFC-Private CAN_Checklist.xlsx"
    #
    # # print(pd.read_excel(signal))
    # dbc = load_dbc(dbc_path)
    # data = read_mf4(path, dbc)
    # print(data.shape)
    # print(extract_wanted_signal_data(data, signal))

    rootpath = "C:\\Users\\Z0050908\\Downloads"
    dbcs = {"Ch3": ['GWM V71 CAN 01C.dbc'], "Ch4": ['FR-IFC-Private CAN.dbc'], "Ch5": ['GWM V71 CAN 01C.dbc'], "Ch6": ['FR-IFC-Private CAN.dbc']}
    path_w = "C:\\Users\\Z0050908\\Documents\\Log_FURel02A01_Reinjection_Rel02A01_0825_offline_release_SW_Data_20200826\\Log_FURel02A01_Reinjection_Rel02A01_0825_offline_release_SW_Data_20200826\\[Reinjection 2] Session_2020_08_26_053716_log_001.mf4"
    path_w_ori = "C:\\Users\\Z0050908\\Documents\\Log_FURel02A01_Reinjection_Rel02A01_0825_offline_release_SW_Data_20200826\\Log_FURel02A01_Reinjection_Rel02A01_0825_offline_release_SW_Data_20200826\\[Original] Traffic_signs_and_Lights_2020_08_20_2020_08_20_060823_log_001.mf4"
    path_g = "C:\\Users\\Z0050908\\Documents\\Reinj_data\\GWM_V71_39_02A01_RB_test_2020_08_26_070508#k826070508q1a24a7\\GWM_V71_39_02A01_RB_test_2020_08_26_070508_log_001.mf4"
    A, B, C = load_total_matrix(rootpath, dbcs)
    # print(A, B, C)

    signal_excel_path = "C:\\Users\\Z0050908\\Desktop\\FR-IFC-Private CAN_Checklist.xlsx"
    signals = pd.read_excel(signal_excel_path)
    wanted = signals[(signals["Priority"] == 1) & (signals["Alignment"] == "Agree")]["Name"]
    dat = loadMF4data2Dict(path_w, wanted, A)
    print(dat.keys())
    for s in wanted:
        try:
            print("exist:", s, dat[s])
        except:
            print("not exist:",  s)
    print(1)

