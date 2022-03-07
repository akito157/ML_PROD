from utils_req import notify_line
from batch_Manager import BatchEach
from batch_Manager import BatchPredict


def do_BatchEach(opdt, mid):
    BatchEach(opdt).main()

def do_BatchPredict(opdt, mid):
    BatchPredict(opdt, mid).main()


def pass_prob_to_mac(opdt, mid):
    pass

import pickle
import sys
# sys.path.append('C:\\Users\\akito157\\.ipython')
# print(sys.path)
sys.path = ['C:\\Users\\akito157\\Projects\\SandBox\\kyotei', 'C:\\Users\\akito157\\AppData\\Local\\Programs\\Python\\Python39\\python39.zip', 'C:\\Users\\akito157\\AppData\\Local\\Programs\\Python\\Python39\\DLLs', 'C:\\Users\\akito157\\AppData\\Local\\Programs\\Python\\Python39\\lib', 'C:\\Users\\akito157\\AppData\\Local\\Programs\\Python\\Python39', 'C:\\Users\\akito157\\Projects\\SandBox\\.venv', '', 'C:\\Users\\akito157\\Projects\\SandBox\\.venv\\lib\\site-packages', 'C:\\Users\\akito157\\Projects\\SandBox\\.venv\\lib\\site-packages\\win32', 'C:\\Users\\akito157\\Projects\\SandBox\\.venv\\lib\\site-packages\\win32\\lib', 'C:\\Users\\akito157\\Projects\\SandBox\\.venv\\lib\\site-packages\\Pythonwin', 'C:\\Users\\akito157\\Projects\\SandBox\\.venv\\lib\\site-packages\\IPython\\extensions', 'C:\\Users\\akito157\\.ipython', '/Users/akito157/Projects/Kyotei']
sys.path.append('Z:/')
# print(len(sys.path))
# print('aaa')
# # model_path = f"/Volumes/TBT3_SSD/Kyotei/Models/a_MODELS_219.pkl"
# model_path = f"/Volumes/TBT3_SSD/Kyotei/Models/a_MODELS_219.pkl"
# model_path = f"Z:/Kyotei/Models/a_MODELS_{219}.pkl"
# pickle.load(open(model_path, 'rb'))
# print(un)

if __name__ == "__main__":
    opdt = '20220306'
    mid ="219"

    # do_BatchEach(opdt, mid)
    do_BatchPredict(opdt, mid)

    pass_prob_to_mac(opdt)

    notify_line('owatta nya-')