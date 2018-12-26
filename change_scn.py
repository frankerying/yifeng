#coding=utf-8
#filename change_scn.py
'''
change csn , rcn and fuzzy

'''
import re
import os
import time
import commands
from optparse import OptionParser
from subprocess import Popen, PIPE

#get the scn
def get_scn():

    max_scn = input("please input the scn:")
    max_16_scn =  hex(max_scn).replace('0x','').replace('L','').zfill(12)

    try:
        sqlplus = Popen(["sqlplus", "/","as","sysdba"], stdout=PIPE, stdin=PIPE)
        sqlplus.stdin.write("select 'thefileis'||name||'isfile' from v$datafile_header where checkpoint_change#<%s;"%max_scn+os.linesep)
        out, err = sqlplus.communicate()
        fix_file = re.findall(r"thefileis(.+?)isfile",out)
    except:
        print "ORACLE not available\n"
        os._exit(0)
    return max_scn,max_16_scn,fix_file

#get the resetlogs_change#
def get_rcn():
    sqlplus = Popen(["sqlplus", "/","as","sysdba"], stdout=PIPE, stdin=PIPE)
    sqlplus.stdin.write("select 'thercnis'||min(resetlogs_change#)||'isrcn' from v$datafile_header;"+os.linesep)
    out, err = sqlplus.communicate()
    min_rcn = re.findall(r"thercnis(.+?)isrcn",out)[0]
    min_16_rcn =  hex(int(min_rcn)).replace('0x','').replace('L','').zfill(12)
    sqlplus = Popen(["sqlplus", "/","as","sysdba"], stdout=PIPE, stdin=PIPE)
    sqlplus.stdin.write("select 'thefileis'||name||'isfile' from v$datafile_header where resetlogs_change#>%s;"%min_rcn+os.linesep)
    out, err = sqlplus.communicate()
    rcn_fix_file = re.findall(r"thefileis(.+?)isfile",out)
    return min_rcn,min_16_rcn,rcn_fix_file

#get the status of fuzzy
def get_fuzzy():
    sqlplus = Popen(["sqlplus", "/","as","sysdba"], stdout=PIPE, stdin=PIPE)
    sqlplus.stdin.write("select 'thefuzis'||name||'isfuz' from v$datafile_header where fuzzy='YES';"+os.linesep)
    out, err = sqlplus.communicate()
    fuz_file = re.findall(r"thefuzis(.+?)isfuz",out)
    return fuz_file
#alter scn
def fix_file(file_name,max_16_scn,size):
    file=open(file_name,'rb+')
    if os.popen("uname").read() == 'Linux\n':
        file.seek(size+484,0)
        for i in range(6)[::-1]:
            file.write(chr(int(max_16_scn[i*2:(i*2+2)],16)))
        file.flush()
    if os.popen("uname").read() == 'AIX\n' or os.popen("uname").read() == 'HP-UX\n':
        high_scn = max_16_scn[0:4]
        low_scn = max_16_scn[4:12]
        file.seek(size+484,0)
        for i in range(4):
            file.write(chr(int(low_scn[i*2:(i*2+2)],16)))
        for i in range(2):
            file.write(chr(int(high_scn[i*2:(i*2+2)],16)))
        file.flush()
    file.close()

#alter resetlogs_change#
def rcn_fix_file(file_name,min_16_rcn,size):
    file=open(file_name,'rb+')
    if os.popen("uname").read() == 'Linux\n':
        file.seek(size+116,0)
        for i in range(6)[::-1]:
            file.write(chr(int(min_16_rcn[i*2:(i*2+2)],16)))
        file.flush()
    if os.popen("uname").read() == 'AIX\n' or os.popen("uname").read() == 'HP-UX\n':
        high_scn = max_16_scn[0:4]
        low_scn = max_16_scn[4:12]
        file.seek(size+116,0)
        for i in range(4):
            file.write(chr(int(low_scn[i*2:(i*2+2)],16)))
        for i in range(2):
            file.write(chr(int(high_scn[i*2:(i*2+2)],16)))
        file.flush()
    file.close()
#alter status of fuzzy 
def fuz_fix_file(fuz_file,size):
    file=open(fuz_file,'rb+')
    file.seek(size+138,0)
    file.write(chr(00))
    file.write(chr(00))
    file.flush()
    file.close()

#check and fix bad block
def dbv(filename,blocksize,bak_path):
    if  os.path.exists(filename) == True:
        output = commands.getoutput("dbv file=%s blocksize=%s"%(filename,blocksize))
        blocknums = re.findall(r"Page (\S+) is marked corrupt",output)
        if blocknums==[]:
            print "\n%s have no corrupt block.\n"%filename
        else:
            print "%s have corrupt block :%s"%(filename,str(','.join(blocknums)))
            for blocknum in blocknums:
                checksum(filename,int(blocknum),int(blocksize),bak_path)
            print "\nYou had repair corrupt block: %s  for  %s \n"%(str(','.join(blocknums)),filename)
    elif os.path.exists(filename) == False:
        print 'Specified FILE (%s) not accessible'%filename

#check the head block for datafile
def head_dbv(filename,blocksize,bak_path):
    if  os.path.exists(filename) == True:
        output = commands.getoutput("dbv file=%s blocksize=%s"%(filename,blocksize))
        blocknums = re.findall(r"Page (\S+) is marked corrupt",output)
        if blocknums==[]:
            pass
        else:
            for blocknum in blocknums:
                checksum(filename,int(blocknum),int(blocksize),bak_path)
        print "%s checksum compute completed.\n"%filename
    elif os.path.exists(filename) == False:
        print 'Specified FILE (%s) not accessible'%filename

#compute and alter the checksum
def checksum(fn,num,size,bak_path):
    file=open(fn,'rb+')
    log_file = open('datafile_fix.log','a+')
    r0=0
    list=[0]
    r1=list*4
    r2=list*4
    r3=list*4
    r4=list*4
    R1=0
    R2=0
    R3=0
    R4=0
    corr=0
    count=0
    out1=list*16
    out2=list*16
    res=list*16
    nul=list*16
    i=0
    ori=list*64
   # file.seek((num)*size,0)
   # basename = os.path.basename(fn)
   # bak_file = open('%s/%s_%s_%s'%(bak_path,basename, time.strftime("%Y_%m_%d_%H_%M_%S", time.localtime()) \
   #     ,num),'w+')
   # bak_file.write(file.read(size))
   # bak_file.close()
    bak_block(fn,num,size,bak_path)
    file.seek((num)*size,0)
#    file.seek(size,0) 
    while count<size :
        str = file.read(64)
        i=0
        while i<64:
            ori[i]=ord(str[i])
            i+=1
        b1=ori[0:16]
        b2=ori[16:32]
        b3=ori[32:48]
        b4=ori[48:64]
        if count==0:
            b2[:2]=[0,0]
        doxor(m=b1,n=b2,out=out1)
        doxor(m=b3,n=b4,out=out2)
        doxor(m=nul,n=out1,out=res)
        nul=res[0:16]

        doxor(m=nul,n=out2,out=res)

        nul=res[0:16]
        count=count+64
    r1=res[0:4]
    r2=res[4:8]
    r3=res[8:12]
    r4=res[12:16]
    R1=r1[0]<<24|r1[1]<<16|r1[2]<<8|r1[3]
    R2=r2[0]<<24|r2[1]<<16|r2[2]<<8|r2[3]
    R3=r3[0]<<24|r3[1]<<16|r3[2]<<8|r3[3]
    R4=r4[0]<<24|r4[1]<<16|r4[2]<<8|r4[3]

    r0=r0^R1
    r0=r0^R2
    r0=r0^R3
    r0=r0^R4

    R1=r0
    r0=r0>>16
    r0=r0^R1
    r0=r0&0xFFFF
    g=(r0&0x00FF)
    r0=r0>>8
    h=(r0&0xFFFF)



    file.seek((num*size+16),0)
    read_h=hex(ord(file.read(1))).replace('0x','').zfill(2)
    read_g=hex(ord(file.read(1))).replace('0x','').zfill(2)
    file.seek((num*size+(size-4)),0)
    b1=hex(ord(file.read(1))).replace('0x','').zfill(2)
    b2=hex(ord(file.read(1))).replace('0x','').zfill(2)
    b3=hex(ord(file.read(1))).replace('0x','').zfill(2)
    b4=hex(ord(file.read(1))).replace('0x','').zfill(2)
    
    file.seek(num*size,0)
    kcbh_str = file.read(32)
    if os.popen("uname").read() == 'Linux\n':
        file.seek((num*size+16),0)
        file.write(chr(h))
        file.seek((num*size+17),0)
        file.write(chr(g))
        file.seek((num*size+size-4),0)
        file.write(kcbh_str[14])
        file.seek((num*size+size-3),0)
        file.write(kcbh_str[0])
        file.seek((num*size+size-2),0)
        file.write(kcbh_str[8])
        file.seek((num*size+size-1),0)
        file.write(kcbh_str[9])
        file.flush()
        file.close()
        log_file.writelines(" %s  %s Block num: %s, Check sum: %s, Block tail flag bit: %s\n"%\
                (time.asctime(time.localtime()),fn,num,read_h+read_g,b1+b2+b3+b4))
        log_file.close()
     #   logging.info(": Block num: %s Check sum: %s, Block tail flag bit: %s"%(num,read_h+read_g,b1+b2+b3+b4))
    elif os.popen("uname").read() == 'AIX\n' or os.popen("uname").read() == 'HP-UX\n':
        file.seek((num*size+16),0)
        file.write(chr(h))
        file.seek((num*size+17),0)
        file.write(chr(g))
        file.seek((num*size+size-4),0)
        file.write(kcbh_str[10])
        file.seek((num*size+size-3),0)
        file.write(kcbh_str[11])
        file.seek((num*size+size-2),0)
        file.write(kcbh_str[0])
        file.seek((num*size+size-1),0)
        file.write(kcbh_str[14])
        file.flush()
        file.close()
        log_file.writelines(" %s  %s Block num: %s, Check sum: %s, Block tail flag bit: %s\n"%\
                (time.asctime(time.localtime()),fn,num,read_h+read_g,b1+b2+b3+b4))
        log_file.close()

#algorithm for checksum
def doxor(m,n,out):
    c=0
    while c<16:
        out[c]=m[c]^n[c]
        c+=1

# recover the datafile
def do_recover(fn,size,num_list,bak_path):
    file_target = open(fn,'rb+')
    basename = os.path.basename(fn)
    bak_file_list = os.popen('ls %s/%s_*'%(bak_path,basename)).read().split('\n')[:-1]
    dict = {}
    dict_time = {}
    for fr in bak_file_list:
        dict_time[os.path.getctime(fr)] = fr
    for fr in bak_file_list:
        if dict.has_key(fr.split('_')[-1]) == True:
            value = min(os.path.getctime(dict[fr.split('_')[-1]]),os.path.getctime(fr))
            dict[fr.split('_')[-1]]=dict_time[value]
        else:
            dict[fr.split('_')[-1]]=fr
    if num_list ==[]:
        for item in dict.items():
            num,fr = item
            try:
                file_rec = open(fr,'rb+')
                file_target.seek(int(num)*size,0)
                file_target.write(file_rec.read(size))
                file_target.flush()
                file_rec.close()
                print "\nRecover datafile :%s \nUndo file:%s\n"%(fn,fr)
            except:
                pass
    else:
        for num_str in num_list:
            try:
                num = int(num_str)
                fr = dict[num_str]
                file_rec = open(fr,'rb+')
                file_target.seek(num*size,0)
                file_target.write(file_rec.read(size))
                file_target.flush()
                file_rec.close()
                print "Recover datafile :%s Undo file:%s"%(fn,fr)
            except:
                print "INPUT ERRO :Block numbers:%s"%num_str 

    file_target.close()
    print "Recover the datefile :%s completed."%fn
    print "------------------------------------------------------------"

#recover the database
def recover_db(datafiles_dict,bak_path):
    sqlplus = Popen(["sqlplus", "/","as","sysdba"], stdout=PIPE, stdin=PIPE)
    sqlplus.stdin.write("select 'thecontrolfileis'||name||'iscontrolfile' from v$controlfile ;"+os.linesep)
    out, err = sqlplus.communicate()
    try:
        control_file = re.findall(r"thecontrolfileis(.+?)iscontrolfile",out)[0]
    except:
        print "Check the database status."
        os._exit(0)
    base_control = os.path.basename(control_file)
   # os.system("cp %s/%s_* %s"%(bak_path,base_control,control_file))
   # print "Recover controlfile completed."
    print "--------------------------------"
    for item in datafiles_dict.items():
        fn = item[0]
        size = int(item[1])
        do_recover(fn,size,[],bak_path)
    print "\nRECOVER DATABASE COMPLETED."

#general 
def run_fix_test(datafiles_dict,V_Wrap,bak_path):
        y_n = raw_input('\nEnsure that you have done the backup of data files, log files, control files, and parameter files ?(y/n)\n')
        if y_n == 'y' or y_n == 'Y':
            print "-----------------------------\nNow start checking and repairing corrupt blocks"
            
            max_scn,max_16_scn,fix_files = get_scn()
            min_rcn,min_16_rcn,rcn_fix_files = get_rcn()
            fuz_files = get_fuzzy()
            fix_files_name = []
            sqlplus = Popen(["sqlplus", "/","as","sysdba"], stdout=PIPE, stdin=PIPE)
            sqlplus.stdin.write("select 'thecontrolfileis'||name||'iscontrolfile' from v$controlfile ;"+os.linesep)
            out, err = sqlplus.communicate()
            try:
                control_file = re.findall(r"thecontrolfileis(.+?)iscontrolfile",out)[0]
            except:
                print "can not get the path of control file."
		os._exit(0)
            base_control = os.path.basename(control_file)
            os.system('cp %s %s/%s_%s'%(control_file,bak_path,base_control,\
			        time.strftime("%Y_%m_%d_%H_%M_%S", time.localtime())))
            print "-----------------------------\nNow modifying the datafile head block information.\n"
            for (file_name,bak_size) in datafiles_dict.items():
                bak_block(file_name,1,bak_size,bak_path)
            time.sleep(1)
            for file_name in fix_files:
                fix_file(file_name,max_16_scn,int(datafiles_dict[file_name]))
                print "Datafile:%s    SCN changed to:%s"%(file_name,max_scn)
            for rcn_file_name in rcn_fix_files:
                rcn_fix_file(rcn_file_name,min_16_rcn,int(datafiles_dict[rcn_file_name]))
            for fuz_file in fuz_files:
                fuz_fix_file(fuz_file,int(datafiles_dict[fuz_file]))
            fix_files_name = list(set(fix_files+rcn_fix_files+fuz_files))
            for fix_file_name in fix_files_name:
                print "%s Recalculating the checksum\n"%fix_file_name
                block_size = int(datafiles_dict[fix_file_name])
                head_dbv(fix_file_name,block_size,bak_path)
            print "-----------------------------\nDetection result:\n"
            sqlplus = Popen(["sqlplus", "/","as","sysdba"], stdout=PIPE, stdin=PIPE)
            sqlplus.stdin.write("select checkpoint_change#,'a_countis'||count(*)||'isa_count' from v$datafile_header group by checkpoint_change#;"+os.linesep)
            out, err = sqlplus.communicate()
            a_count = re.findall(r"a_countis(.+?)isa_count",out)
            sqlplus = Popen(["sqlplus", "/","as","sysdba"], stdout=PIPE, stdin=PIPE)
            sqlplus.stdin.write("select fuzzy,'b_countis'||count(*)||'isb_count' from v$datafile_header group by fuzzy;"+os.linesep)
            out, err = sqlplus.communicate()
            b_count = re.findall(r"b_countis(.+?)isb_count",out)
            sqlplus = Popen(["sqlplus", "/","as","sysdba"], stdout=PIPE, stdin=PIPE)
            sqlplus.stdin.write("select resetlogs_change#,'c_countis'||count(*)||'isc_count' from v$datafile_header group by resetlogs_change#;"+os.linesep)
            out, err = sqlplus.communicate()
            c_count = re.findall(r"c_countis(.+?)isc_count",out)
            time.sleep(2)
            if len(a_count)+len(b_count)+len(c_count) == 3:
		            print "Running Successfull"
            else:
                print a_count,b_count,c_count
                print "Running Failed"
        else:
            os._exit(0)
def bak_block(fn,num,size,bak_path):
    size = int(size)
    file=open(fn,'rb+')
    file.seek((num)*int(size),0)
    basename = os.path.basename(fn)
    bak_file = open('%s/%s_%s_%s'%(bak_path,basename, time.strftime("%Y_%m_%d_%H_%M_%S", time.localtime()) \
        ,num),'wb+')
    bak_file.write(file.read(size))
    bak_file.close()
    file.close()
    print "Datafile:%s      backup block id:%s."%(fn,num) 


if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option("-f", action="store_true", dest="step",
                  help="FIX THE DATABASE.") 
    parser.add_option("-r", action="store_false",dest="step",
                  help="RECOVER THE DATABASE.")

    (options, args) = parser.parse_args()

    bak_path = '/tmp/bak_force_fix'
    if os.path.exists(bak_path) == True:
        pass
    else:
        os.makedirs(bak_path)

    sqlplus = Popen(["sqlplus", "/","as","sysdba"], stdout=PIPE, stdin=PIPE)
    sqlplus.stdin.write("select 'thefileis'||name||'isfile','thebsis'||block_size||'isbs' from v$datafile;"+os.linesep)
    out, err = sqlplus.communicate()
    data_file = re.findall(r"thefileis(.+?)isfile",out)
    block_sizes = re.findall(r"thebsis(.+?)isbs",out)
    datafiles_dict = dict(zip(data_file,block_sizes))
    if datafiles_dict == {"'||name||'": "'||block_size||'"}:
        print "ORACLE not available\n"
        os._exit(0)
    if options.step == True or  options.step == None : 
        run_fix_test(datafiles_dict,0,bak_path)
    elif options.step == False :
        recover_db(datafiles_dict,bak_path)


