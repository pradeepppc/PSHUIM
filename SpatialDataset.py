from Transaction import Transaction


class Dataset:
    transactions = []
    maxItem = 0

    def __init__(self, datasetpath, neighbors):
        with open(datasetpath, 'r') as f:
            lines = f.readlines()
            for line in lines:
                self.transactions.append(self.createTransaction(line, neighbors))
        print('Transaction Count :' + str(len(self.transactions)))
        f.close()

    def createTransaction(self, line, neighbors):
        trans_list = line.strip().split(':')
        transactionUtility = int(trans_list[1])
        itemsString = trans_list[0].strip().split(' ')
        utilityString = trans_list[2].strip().split(' ')
        # pmuString = trans_list[3].strip().split(' ')
        items = []
        utilities = []
        pmus = []
        for idx, item in enumerate(itemsString):
            item_int = int(item)
            if item_int > self.maxItem:
                self.maxItem = item_int
            items.append(item_int)
            utilities.append(int(utilityString[idx]))
            pm = int(utilityString[idx])
            if item_int in neighbors:
                for j in range(0, len(itemsString)):
                    if j != idx:
                        if int(itemsString[j]) in neighbors[item_int]:
                            pm += int(utilityString[j])

            pmus.append(pm)
        return Transaction(items, utilities, transactionUtility, pmus)

    def getMaxItem(self):
        return self.maxItem

    def getTransactions(self):
        return self.transactions
