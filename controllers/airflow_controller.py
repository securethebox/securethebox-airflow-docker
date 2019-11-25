import subprocess
import os
import json

class AirflowController():

    def __init__(self):
        self.jsonDynamicPoolFilePath = str(os.getcwd())+"/pools/dynamic-pools.json"
        self.jsonPoolData = []
    
    def loadDynamicPools(self):
        if os.stat(self.jsonDynamicPoolFilePath).st_size != 0:
            with open(self.jsonDynamicPoolFilePath, 'r') as f:
                self.jsonPoolData = json.load(f)

    def addDynamicPool(self, poolName, slots):
        jdict = {}
        jdict[poolName] = { "description": str(poolName)+" pool", "slots": slots }
        existingPools = any(poolName in d for d in self.jsonPoolData)
        # If pool name does not exist, add pool
        if existingPools != True:
            self.jsonPoolData.append(jdict)
            with open(self.jsonDynamicPoolFilePath, 'w+') as f:
                json.dump(self.jsonPoolData, f)
        # If pool name does exist, update pool
        else:
            for i,x in enumerate(self.jsonPoolData):
                if poolName in x:
                    self.jsonPoolData[i][poolName]['slots'] = slots
                    with open(self.jsonDynamicPoolFilePath, 'w+') as f:
                        json.dump(self.jsonPoolData, f)
                
    
    def importDynamicPoolFile(self):
        command = "airflow pool -i "+os.getcwd()+"/pools/dynamic-pools.json"
        subprocess.Popen(command,shell=True)
        print("Should load new Pools")

if __name__ == "__main__":
    ac = AirflowController()
    ac.loadDynamicPools()
    ac.addDynamicPool("te1st", 5)
    ac.addDynamicPool("te2st", 4)
    ac.addDynamicPool("te2st", 4)
    ac.addDynamicPool("te2st", 4)
    ac.addDynamicPool("te2st", 4)
    ac.addDynamicPool("te2st", 1)
    ac.addDynamicPool("te3aaaast", 1)
    ac.addDynamicPool("te1st", 1)