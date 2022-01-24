import { HardhatRuntimeEnvironment } from 'hardhat/types';
import { ethers, network } from 'hardhat';
import { DeployFunction } from 'hardhat-deploy/types';
const TEST_MONEY = ethers.utils.parseEther('100000');
const DECIMALS = ethers.utils.parseEther('1');


const func: DeployFunction = async function (hre: HardhatRuntimeEnvironment) {
    const { deployments, getNamedAccounts } = hre;
    const { deploy } = deployments;

    const { deployer } = await getNamedAccounts();
    console.log(deployer)


    const isOracleNew = (await deploy('MerkleDistributor', {
        from: deployer,
        args: ['0x96aaea16a6b418f92ac445af7c98da897e035ea230510a187567e19895ad9157'],
    })).newlyDeployed;
    
};
export default func;
func.tags = ["merkle"]