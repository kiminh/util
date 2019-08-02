BEGIN{FS="\001";OFS="\001"}{
    label=$1;
    #print label
    if(label==1){
        print
    }else{
        #print rand()
        if(rand()<0.1){
            print
        }       
        
    }

}
