'''
gen-accounts.py
sonicskye@2018

the file contains functions to generate accounts and store them to a database

'''

# shasta network
from vars import shastaFullNode, shastaSolidityNode, shastaEventServer
from vars import shastaFullNodeAddress, shastaSolidityNodeAddress, shastaEventServerAddress
# mainnet
from vars import fullNode, solidityNode, eventServer
from vars import fullNodeAddress, solidityNodeAddress, eventServerAddress

from vars import dbName
from utilities import quotedstr, sha1file, sha1string
from tronapi import Tron
import os, sqlite3, math, binascii
from splitjoin import split, join, natural_sort


DB_DEFAULT_PATH = os.path.join(os.path.dirname(__file__), dbName)
DEFAULT_TOKEN_NAME = 'TOKEN'
DEFAULT_ADDRESS = 'ADDRESS'
#define constants
# the maximum payload is 2350 because every two characters represents one byte data
#_MAX_PAYLOAD = 4700
MAX_PAYLOAD = 2350
TX_SIZE = 300
TEMP_FOLDER = os.path.join(os.path.dirname(__file__), 'tmp')
TEMP_FOLDER_READ = os.path.join(os.path.dirname(__file__), 'tmpread')
RESULT_FOLDER = os.path.join(os.path.dirname(__file__), 'results')

tron = Tron(full_node=fullNode,
            solidity_node=solidityNode,
            event_server=eventServer)

shastaTron = Tron(full_node=shastaFullNode,
            solidity_node=shastaSolidityNode,
            event_server=shastaEventServer)

# @dev dbconnect connects to a database stated in DB_DEFAULT_PATH
# https://stackabuse.com/a-sqlite-tutorial-with-python/
def dbconnect(dbPath=DB_DEFAULT_PATH):
    con = sqlite3.connect(dbPath)
    return con


# @dev genaccounts generate a number of accounts and store them to taccounts table in the database
def genaccounts(numAccounts, network="SHASTA"):
    # choose tron based on the network
    if network.upper() == "MAINNET":
        tr = tron
    else:
        tr = shastaTron

    #connect to database
    cnx = dbconnect()
    cur = cnx.cursor()


    for i in range(0, numAccounts):
        account = tr.create_account
        address = account.address.base58
        privateKey = account.private_key
        publicKey = account.public_key
        addressHex = account.address.hex
        s = 'INSERT INTO taccounts(address, privkey, pubkey, address_hex, network) VALUES (%s,%s,%s,%s,%s)'
        s = s % (quotedstr(str(address)), quotedstr(str(privateKey)), quotedstr(str(publicKey)), quotedstr(str(addressHex)), quotedstr(network))
        #print(s)
        try:
            cur.execute(s)
            cnx.commit()

            print(i + " : " + str(address))
        except:
            cnx.rollback()

    cnx.close()


# @dev see tronapi/trx.py line 349
"""Transfer Token
def send_token(self, to, amount, token_id=None, account=None):
        Args:
            to (str): is the recipient address
            amount (float): is the amount of token to transfer
            token_id (str): Token Name(NOT SYMBOL)
            account: (str): is the address of the withdrawal account

        Returns:
            Token transfer Transaction raw data

        """
def sendtoken(toAddress, amount, tokenName, sender, privKeySender, fromAddress=None, message = None, network='SHASTA'):
    # choose tron based on the network
    if network.upper() == "MAINNET":
        tr = tron
    else:
        tr = shastaTron

    tr.private_key = privKeySender
    tr.default_address = sender

    if not message:
        send = tr.trx.send_token(toAddress, amount, tokenName, fromAddress)
    else:
        send = tr.trx.send_token(toAddress, amount, tokenName, fromAddress, {'message': message})
    return send


# @dev sendpayload sends the data chunk
# @dev the sender is chosen from the database, based on the remaining bandwidth
# @dev the receiver is the address next to the sender (based on the id) (circular order)
def sendpayload(dataHash=None, chunkIdx=0, message=None, network='SHASTA'):
    # amount of tokens to be paid is 1
    amount = 1
    tokenName = DEFAULT_TOKEN_NAME

    # get the next available account based on remaining bandwidth
    messageLength = len(message)
    messageHash = sha1string(message)
    bandwidthRequired = messageLength + 300

    # connect to database
    cnx = dbconnect()
    cur = cnx.cursor()

    #print(bandwidthRequired)

    # offset is to enable threading so that multiple chunks can be processed at the same time
    # this ensures different accounts are picked for different chunkIdx
    # chunkIdx starts from 1, but offset starts from 0, therefore the offset is chunkIdx - 1
    s = 'SELECT address, id, privkey FROM taccounts WHERE initialised = 1 AND network = %s AND bandwidth_remaining >= %s ORDER BY bandwidth_remaining ASC, id ASC LIMIT 1 OFFSET %s'
    s = s % (quotedstr(network), str(bandwidthRequired), str(chunkIdx-1))
    #print(s)
    cur.execute(s)
    row = cur.fetchone()
    isSuccessful = False
    if row:
        addressSender = row[0]
        id = row[1]
        senderPrivKey = row[2]

        #get the destination address, which should be the address of id+1
        s = 'SELECT address FROM taccounts WHERE id = %s AND network = %s'
        s = s % (str(id + 1), quotedstr(network))
        cur.execute(s)
        row2 = cur.fetchone()
        # if there is a next address
        if len(row2) > 0:
            addressReceiver = row2[0]
        # if next address is not available, then go back to the first address
        else:
            # get the destination address, which should be the address of id=1
            s = 'SELECT address FROM taccounts WHERE id = 1 AND network = %s'
            s = s % (str(id + 1), quotedstr(network))
            cur.execute(s)
            row3 = cur.fetchone()
            if len(row3) > 0:
                addressReceiver = row3[0]
            else:
                addressReceiver = DEFAULT_ADDRESS

        result = sendtoken(addressReceiver, amount, tokenName, addressSender, senderPrivKey, addressSender, message, network)
        try:
            isSuccessful = result['result']
        except:
            isSuccessful = False

        if isSuccessful:
            txID = result['transaction']['txID']
            # store the data to the database
            s = 'INSERT INTO ttransactions(txhash, address, data_hash, chunk_idx, chunk_size, chunk_hash, network) ' \
                'VALUES (%s, %s, %s, %s, %s, %s, %s)'
            s = s % (quotedstr(txID), quotedstr(addressSender), quotedstr(dataHash), str(chunkIdx), str(messageLength),
                     quotedstr(messageHash), quotedstr(network))
            try:
                cur.execute(s)
                cnx.commit()

                print('Chunk index ' + str(chunkIdx) + ' has been successfully stored to the blockchain and the record to the database. TxID: ' + txID)
            except:
                cnx.rollback()
                print('Chunk index ' + str(chunkIdx) + ' has been successfully stored to the blockchain but recording to the database failed. TxID: ' + txID)
        else:
            print('Chunk index ' + str(chunkIdx) + ' failed to be stored to the blockchain')
            print(result)


    else:
        print('No account with sufficient bandwidth')

    return isSuccessful


# @dev initialise a number of account by sending them a number of token
def initaccount(numAccount, tokenAmount, tokenName, sender, privKeySender, network='SHASTA'):
    # choose tron based on the network
    if network.upper() == "MAINNET":
        tr = tron
    else:
        tr = shastaTron

    # connect to database
    cnx = dbconnect()
    cur = cnx.cursor()

    # select address from TACCOUNTS where initialised = 0 order by id limit 10
    s = 'SELECT address, id FROM taccounts WHERE initialised = 0 AND network = %s ORDER BY id LIMIT %s'
    s = s % (quotedstr(network), str(numAccount))
    cur.execute(s)
    rows = cur.fetchall()
    for row in rows:
        receiver = row[0]
        id = row[1]
        txt = "SENARAI INITIALISATION. ID: " + str(id)
        # the result is already in dictionary format
        result = sendtoken(receiver, tokenAmount, tokenName, sender, privKeySender, fromAddress=None, message=txt, network=network)
        # print(result)
        try:
            isSuccessful = result['result']
            txID = result['transaction']['txID']
        except:
            isSuccessful = False
            print(result)


        #if transaction is successful, then update status INITIALISED to 1 as well as save the txid
        if isSuccessful:
            s = 'UPDATE taccounts SET initialised = 1 WHERE address = %s AND network = %s'
            s = s % (quotedstr(receiver), quotedstr(network))
            try:
                cur.execute(s)
                cnx.commit()

                print('Address ' + str(receiver) + ' is initialised by receiving ' + str(tokenAmount) + ' ' + tokenName)
            except:
                cnx.rollback()

            s = 'INSERT INTO taccountinit(txhash, address, network) VALUES (%s, %s, %s)'
            s = s % (quotedstr(txID), quotedstr(receiver), quotedstr(network))
            try:
                cur.execute(s)
                cnx.commit()
            except:
                cnx.rollback()

    cnx.close()


# @dev getpayload gets the saved payload from transaction information
def getpayload(txid, network='SHASTA'):
    # choose tron based on the network
    if network.upper() == "MAINNET":
        tr = tron
    else:
        tr = shastaTron
    data = tr.trx.get_transaction(txid)
    try:
        # the payload is by default decoded to Hex (utf-8)
        payload = data['raw_data']['data']
    except:
        payload = None
    return payload


# @dev getremainingbandwidth gets information about the address' remaining bandwidth
def getremainingbandwidth(address, network='SHASTA'):
    # choose tron based on the network
    if network.upper() == "MAINNET":
        tr = tron
    else:
        tr = shastaTron
    data = tr.trx.get_band_width(address)
    try:
        usedBandwidth = data['freeNetUsed']
    except:
        usedBandwidth = 0
    try:
        freeBandwidth = data['freeNetLimit']
    except:
        freeBandwidth = 0
    return freeBandwidth - usedBandwidth


# @dev updatebandwidthremaining calculates total bandwidth remaining from all accounts
# @dev and update the information to the database
def updatebandwidthremaining(network='SHASTA'):
    # choose tron based on the network
    if network.upper() == "MAINNET":
        tr = tron
    else:
        tr = shastaTron

    # connect to database
    cnx = dbconnect()
    cur = cnx.cursor()

    s = 'SELECT address FROM taccounts WHERE initialised = 1 AND network = %s ORDER BY id ASC'
    s = s % (quotedstr(network))
    cur.execute(s)
    rows = cur.fetchall()
    totalRemainingBandwidth = 0
    totalData = len(rows)
    progress = 0
    for row in rows:
        address = row[0]
        remainingBandwidth = getremainingbandwidth(address, network)
        # store the remaining bandwidth to the respective account
        s = 'UPDATE taccounts SET bandwidth_remaining = %s WHERE address = %s AND network = %s'
        s = s % (str(remainingBandwidth), quotedstr(address), quotedstr(network))
        try:
            cur.execute(s)
            cnx.commit()
        except:
            cnx.rollback()

        totalRemainingBandwidth = totalRemainingBandwidth + remainingBandwidth
        progress += 1
        print('Update in Progress: ' + str(int(progress / totalData * 100)) + '%')

    # update to database
    s = 'REPLACE INTO tbandwidth_remaining (bandwidth, network) VALUES (%s, %s)'
    s = s % (str(totalRemainingBandwidth), quotedstr(network))
    try:
        cur.execute(s)
        cnx.commit()
        print('Updated remaining Bandwidth on ' + network + ': ' + str(totalRemainingBandwidth) + ' bytes')
    except:
        cnx.rollback()

    #close the database
    cnx.close()


# @dev savedata stores the data to the blockchain
def savedata(fileName, network='SHASTA'):

    # assuming that the remaining bandwidth is up to date
    # check the file size and compare it to the remaining bandwidth
    # connect to database
    cnx = dbconnect()
    cur = cnx.cursor()
    s = 'SELECT bandwidth FROM tbandwidth_remaining WHERE network = %s'
    s = s % (quotedstr(network))
    cur.execute(s)
    row = cur.fetchone()
    if len(row) > 0:
        remainingBandwidth = float(row[0])
    else:
        remainingBandwidth = 0

    # get file size
    fileSize = os.path.getsize(fileName)

    # get the file hash
    fileHash = sha1file(fileName)

    # compute the number of transactions required
    numTx = int(math.ceil(fileSize/MAX_PAYLOAD))
    totalSize = float(fileSize + (numTx * TX_SIZE))

    # totalSize of the required bandwidth should be less or equal to 45% of remainingBandwidth, for safety
    # because the totalSize will double in size when stored as Hex in the transactions
    usableBandwidth = remainingBandwidth * 0.45
    if totalSize <= usableBandwidth:
        print('Bandwidth is sufficient. Process continues...')
        # save the file information to the database
        s = 'REPLACE INTO tstoreddata(hash, name, size, network) VALUES (%s, %s, %s, %s)'
        s = s % (quotedstr(fileHash), quotedstr(fileName), str(totalSize), quotedstr(network))
        fileOK = False
        try:
            cur.execute(s)
            cnx.commit()
            fileOK = True
        except:
            cnx.rollback()
            fileOK = False

        if fileOK:
            # process
            # 1: splits the file into temporary folders
            split(fileName, TEMP_FOLDER, MAX_PAYLOAD)
            # 2: processes each splitted chunks to the database and to the blockchain
            # Get a list of the file parts
            parts = os.listdir(TEMP_FOLDER)

            # Sort them by name (remember that the order num is part of the file name)
            parts = natural_sort(parts)

            # Go through each portion one by one
            chunkIdx = 0
            for file in parts:
                chunkIdx += 1
                print('Processing chunk index: ' + str(chunkIdx))
                # Assemble the full path to the file
                path = os.path.join(TEMP_FOLDER, file)
                # Open the part in bytes
                input_file = open(path, 'rb')
                # Open the part in string
                chunkData = input_file.read(MAX_PAYLOAD)
                chunkDataString = binascii.hexlify(chunkData).decode('utf-8')
                isChunkStored = False;
                isChunkStored = sendpayload(fileHash, chunkIdx, chunkDataString, network)
                # if successfull, remove the chunk
                if isChunkStored:
                    os.remove(path)

    else:
        print('Insufficient bandwidth. Available: ' + str(remainingBandwidth) + ', required: ' + str(totalSize * 2)
              + ', diff: ' + str(totalSize * 2 - remainingBandwidth))

    cnx.close()


def readdata(fileHash, network='SHASTA'):
    # choose tron based on the network
    if network.upper() == "MAINNET":
        tr = tron
    else:
        tr = shastaTron

    # connect to database
    cnx = dbconnect()
    cur = cnx.cursor()

    s = 'SELECT name FROM tstoreddata WHERE hash = %s AND network = %s'
    s = s % (quotedstr(fileHash), quotedstr(network))
    cur.execute(s)
    row = cur.fetchone()
    if len(row) > 0:
        fileName = row[0]

    s = 'SELECT txhash, chunk_idx, chunk_size, chunk_hash FROM ttransactions WHERE data_hash = %s AND network = %s ORDER BY chunk_idx ASC'
    s = s % (quotedstr(fileHash), quotedstr(network))
    cur.execute(s)
    rows = cur.fetchall()
    for row in rows:
        txHash = row[0]
        chunkIdx = row[1]
        chunkSize = row[2]
        chunkHash = row[3]
        # convert Tron's default toHex when creating transaction data to text using toText
        chunkDataString = tr.toText(getpayload(txHash, network))
        # decode the payload then save as the chunk file
        originalData = binascii.unhexlify(chunkDataString.encode('utf-8'))
        #print(originalData)
        # Create a new file name
        filename = os.path.join(TEMP_FOLDER_READ, ('part-' + str(chunkIdx)))
        # Create a destination file
        dest_file = open(filename, 'wb')
        # Write to this portion of the destination file
        dest_file.write(originalData)

        # Explicitly close
        dest_file.close()

    resultPath = os.path.join(RESULT_FOLDER, fileName)
    join(TEMP_FOLDER_READ, resultPath, MAX_PAYLOAD)

    #remove all files
    parts = os.listdir(TEMP_FOLDER_READ)
    for file in parts:
        path = os.path.join(TEMP_FOLDER_READ, file)
        os.remove(path)

    print('Reading file finished')


def main():
    # generate new accounts
    genaccounts(10000, "MAINNET")

    # initialise account
    #initaccount(numaccount, numtoken, DEFAULT_TOKEN_NAME, DEFAULT_ADDRESS, PRIVKEY_DEFAULT_ADDRESS, 'MAINNET')

    # get remaining bandwidth
    #print(getremainingbandwidth('ACCOUNT', 'MAINNET'))

    # get payload
    #print(getpayload('TxID', 'MAINNET'))

    # update remaining bandwidth
    #updatebandwidthremaining('MAINNET')

    # store
    #fileName = 'cryptocurrency.jpg'
    #filePath = os.path.join(os.path.dirname(__file__), fileName)
    #savedata(fileName, 'MAINNET')

    # read data
    #readdata('SHA1 value of the file','MAINNET')

if __name__ == '__main__':
    main()