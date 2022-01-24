import { BigNumber, utils } from 'ethers'
import BalanceTree from './balance-tree'

const { isAddress, getAddress } = utils

// This is the blob that gets distributed and pinned to IPFS.
// It is completely sufficient for recreating the entire merkle tree.
// Anyone can verify that all air drops are included in the tree,
// and the tree has no additional distributions.
interface MerkleDistributorInfo {
  merkleRoot: string
  tokenTotal: string
  claims: {
    [account: string]: {
      index: number
      score: number 
      proof: string[]
      creature: string
    }
  }
}

type OldFormat = { [account: string]: number | string }
type NewFormat = { address: string; score: number; creature: string }

export function parseBalanceMap(balances: OldFormat | NewFormat[]): MerkleDistributorInfo {
  // if balances are in an old format, process them
  const balancesInNewFormat: NewFormat[] = Array.isArray(balances)
    ? balances
    : Object.keys(balances).map(
        (account): NewFormat => ({
          address: account,
          score: 0,
          creature: '',
        })
      )
  const dataByAddress = balancesInNewFormat.reduce<{
    [address: string]: { score: number; creature: string }
  }>((memo, { address: account, score, creature}) => {
    if (!isAddress(account)) {
      throw new Error(`Found invalid address: ${account}`)
    }
    const parsed = getAddress(account)
    if (memo[parsed]) throw new Error(`Duplicate address: ${parsed}`)
    memo[parsed] = { score: score, creature: creature}
    return memo
  }, {})
  const sortedAddresses = Object.keys(dataByAddress).sort()

  // construct a tree
  const tree = new BalanceTree(
    sortedAddresses.map((address) => ({ account: address, score: dataByAddress[address].score, creature: dataByAddress[address].creature}))
  )

  // generate claims
  const claims = sortedAddresses.reduce<{
    [address: string]: { score: number; index: number; proof: string[]; creature: string}
  }>((memo, address, index) => {
    const { score, creature} = dataByAddress[address]
    memo[address] = {
      index,
      score,
      proof: tree.getProof(index, address, score, creature),
      creature
    }
    return memo
  }, {})

  const tokenTotal: BigNumber = sortedAddresses.reduce<BigNumber>(
    (memo, key) => memo.add(dataByAddress[key].score),
    BigNumber.from(0)
  )

  return {
    merkleRoot: tree.getHexRoot(),
    tokenTotal: tokenTotal.toHexString(),
    claims,
  }
}
