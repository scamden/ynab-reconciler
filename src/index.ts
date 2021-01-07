import * as csv from 'csv-parser';
import * as dayjs from 'dayjs';
import { Decimal } from 'decimal.js';
import * as fs from 'fs';
import * as _ from 'lodash';
import * as numeral from 'numeral';
import * as uuid from 'uuid';

const SPLIT_REGEX = /Split \((\d)+\/(\d)+\)/;

type ReconcileOpts = {
  findInYnabNotInBank: boolean;
  logYnabTransactionForAccount: boolean;
  earliestDate: string;
};

export async function reconcile(ynabCsvPath: string, bankCsvPathString: string, accountName: string | RegExp, opts?: ReconcileOpts) {
  const reconcileOpts: ReconcileOpts = {
    findInYnabNotInBank: false,
    logYnabTransactionForAccount: false,
    earliestDate: dayjs().subtract(1, 'year').format(),
    ...opts,
  };
  const { findInYnabNotInBank, earliestDate, logYnabTransactionForAccount } = reconcileOpts;
  const earliestDateDay = dayjs(earliestDate);
  const parsedBank = await parseCsvFile(bankCsvPathString);
  const bankTrans = sortByDate(getBankTransactions(parsedBank)).filter((tran) => !tran.date.isBefore(earliestDateDay));
  const ynabTrans = sortByDate(getYnabTransactions(await parseCsvFile(ynabCsvPath), accountName)).filter(
    (tran) => !tran.date.isBefore(earliestDateDay)
  );
  if (logYnabTransactionForAccount) {
    logTransactions(ynabTrans);
  }
  const ynabByAmount = _.groupBy(ynabTrans, 'amount');
  const bankByAmount = _.groupBy(bankTrans, 'amount');
  const ynabAmounts = _.keys(ynabByAmount);
  const bankAmounts = _.keys(bankByAmount);
  const bothAmounts = _.intersection(ynabAmounts, bankAmounts);
  const onlyYnabAmounts = _.difference(ynabAmounts, bothAmounts);
  const onlyBankAmounts = _.difference(bankAmounts, bothAmounts);
  const mismatches = bothAmounts.reduce((accum, amount) => {
    const bankTransForAmount = bankByAmount[amount];
    const ynabTransForAmount = ynabByAmount[amount];
    if (findInYnabNotInBank && ynabTransForAmount.length <= bankTransForAmount.length) {
      return accum;
    }
    if (!findInYnabNotInBank && bankTransForAmount.length <= ynabTransForAmount.length) {
      return accum;
    }
    return [...accum, { bank: bankTransForAmount, ynab: ynabTransForAmount }];
  }, []);
  if (onlyYnabAmounts.length && findInYnabNotInBank) {
    console.log('In Ynab but not in Bank');
    logTransactions(onlyYnabAmounts.reduce((trans, amount) => [...trans, ...ynabByAmount[amount]], []));
  }
  if (onlyBankAmounts.length && !findInYnabNotInBank) {
    console.log('In Bank but not in Ynab');
    logTransactions(onlyBankAmounts.reduce((trans, amount) => [...trans, ...bankByAmount[amount]], []));
  }
  if (mismatches.length) {
    console.log('Found mismatched lengths for the following');
    mismatches.forEach(({ bank, ynab }) => {
      console.log('\nMismatch:');
      const longer = bank.length > ynab.length ? bank : ynab;
      const other = bank.length > ynab.length ? ynab : bank;
      const likelyMatches = other.map((otherTran) => {
        const closestTran = longer.reduce(
          (closest: Transaction | null, longerTran) =>
            !closest || distanceBetweenDates(closest.date, otherTran.date) > distanceBetweenDates(longerTran.date, otherTran.date)
              ? longerTran
              : closest,
          null
        );
        return [otherTran, closestTran!];
      });
      console.log('Likely Matches:');
      likelyMatches.forEach((likelyMatch) => {
        console.log('');
        logTransactions(likelyMatch);
      });

      console.log('\nProbable mismatches');
      logTransactions(
        _.differenceBy(
          longer,
          likelyMatches.map((lm) => lm[1]),
          'id'
        )
      );
    });
  }
}

// not
function differenceScore(t1: Transaction, t2: Transaction) {
  return distanceBetweenDates(t1.date, t2.date);
}

function distanceBetweenDates(d1: dayjs.Dayjs, d2: dayjs.Dayjs) {
  return Math.abs(new Decimal(d1.valueOf()).sub(d2.valueOf()).toNumber());
}

export function logTransactions(trans: Transaction[]) {
  trans.forEach(({ date, payee, amount, source }) => {
    console.log(`${source} | ${date.format('MM/DD/YYYY')} | ${payee} | ${numeral(amount).format('$0,0.00')}`);
  });
}

export type Transaction = {
  id: string;
  date: dayjs.Dayjs;
  payee: string;
  amount: number;
  source: 'Bank' | 'Ynab';
};

export type YnabRow = {
  Date: string;
  Payee: string;
  Outflow: string;
  Inflow: string;
  '"Account"': string;
  Memo: string;
};

export type BankRow = {
  'Transaction Date': string;
  Description: string;
  Amount?: string;
  Debit: string;
  Credit: string;
};

export const sortByDate = (trans: Transaction[]) => trans.sort((a, b) => (dayjs(a.date).isAfter(dayjs(b.date)) ? 1 : -1));

export function getBankTransactions(parsedCsv: BankRow[]): Transaction[] {
  return parsedCsv.map(({ Description, Debit, Credit, Amount, 'Transaction Date': date }) => ({
    id: uuid.v4(),
    date: dayjs(date),
    payee: Description,
    source: 'Bank',
    amount:
      (Amount && getNumberFromDollarString(Amount || '')) ||
      (Debit && -1 * getNumberFromDollarString(Debit || '')) ||
      getNumberFromDollarString(Credit || ''),
  }));
}

export function getYnabTransactions(parsedCsv: YnabRow[], accountName: string | RegExp): Transaction[] {
  return parsedCsv
    .filter((tran) => {
      const account = _.find(tran, (v, k) => {
        return k.includes('Account');
      });
      if (!account) {
        throw new Error('NOT ACCOUNT');
      }
      return typeof accountName === 'string' ? account.toLowerCase() === accountName.toLowerCase() : accountName.test(account);
    })
    .map(({ Payee, Outflow, Inflow, Date: date, Memo }) => ({
      id: uuid.v4(),
      date: dayjs(date),
      payee: Payee,
      source: 'Ynab' as 'Ynab',
      amount: (Outflow && -1 * getNumberFromDollarString(Outflow)) || getNumberFromDollarString(Inflow),
      mergeWithPrevious: mergeableSplit(Memo),
    }))
    .reduce<Transaction[]>((trans, tran) => {
      if (tran.mergeWithPrevious) {
        const previous = _.last(trans);
        if (!previous) {
          throw new Error(`said to merge previous but no previous found, ${JSON.stringify(tran)}`);
        }
        return [
          ...trans.slice(0, trans.length - 1),
          {
            ...previous,
            amount: new Decimal(previous.amount).add(tran.amount).toNumber(),
          },
        ];
      }
      return [...trans, tran];
    }, []);
}

function mergeableSplit(memo: string) {
  const matches = SPLIT_REGEX.exec(memo);
  if (matches && matches[1]) {
    const splitCount = parseInt(matches[1], 10);
    return splitCount && splitCount > 1;
  }
  return false;
}

export function getNumberFromDollarString(dollars: string) {
  return parseFloat(dollars.replace('$', ''));
}

export function parseCsvFile(filePath: string) {
  const results: any[] = [];
  return new Promise<any[]>((resolve) => {
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (data) => results.push(data))
      .on('end', () => {
        resolve(results);
      });
  });
}
