##py2.7
import os
import re
import imaplib
import email
import smtplib
import pymysql
import create_incid_ws
import create_reqid_ws
import exclude_emaillist_bmc
import Constants
import base64
from email.utils import getaddresses
import time

def forward_mail(message,msg_from,msg_to,msg_sub,mail,idl):
    message.replace_header("From", str(msg_from))
    message.replace_header("To", str(msg_to))
    message.replace_header("Subject", str(msg_sub))

    smtp = smtplib.SMTP(Constants.SMTP_HOST,Constants.SMTP_PORT)
    smtp.ehlo()
    smtp.starttls()
    smtp.ehlo()    
    smtp.login(Constants.EMAIL_USER,base64.b64decode(Constants.EMAIL_PASSWORD))
    smtp.sendmail(msg_from, Constants.FWD_MAIL, message.as_string())
    smtp.quit()

    print "mail forwarded " +str(msg_sub)

    mail.uid('STORE', idl, '+FLAGS', '(\Seen)')


def get_request_details(incident_id,thid,cur):
    ###call webservice to get request# and status
    details = create_reqid_ws.create_request_id(incident_id)
    req_id = details[0].encode('utf8') ##convert unicode request_id to string
    status = details[1].encode('utf8') ##convert unicode status to string
    
    ###update db with request id and status
    update_dets = "UPDATE mail_details SET request_id = '"+req_id+"' , status = '"+status+"' where thread_id = '"+thid+"';"
    cur.execute(update_dets)

    print "details updated for " +thid

    return req_id

def get_to_cc(msg_to,msg_cc,exlist):
    cc_list = []
    
    msg_to = msg_to.split(',')
    if msg_cc:
        msg_cc = msg_cc.split(',')
    
    for mt in msg_to:
        mt1 = mt[mt.find("<")+1:mt.find(">")]
        cc_list.append(mt1)
    if msg_cc:
        for mc in msg_cc:
            mc1 = mc[mc.find("<")+1:mc.find(">")]
            cc_list.append(mc1)

    for exl in exlist:
        for el in exl:
            for cl in cc_list:
                if el == cl:
                    cc_list.remove(cl)
                    
    new_msg_to = Constants.MAILBOX
    
    return new_msg_to, cc_list
   
def main():
    conn = pymysql.connect(host=Constants.DB_HOST,port=Constants.DB_PORT,user=Constants.DB_USER,passwd=base64.b64decode(Constants.DB_PASSWORD),db=Constants.DATABASE)
    cur = conn.cursor()
    #cur.execute("CREATE TABLE IF NOT EXISTS mail_details (thread_id VARCHAR(50) PRIMARY KEY, incident_id VARCHAR(50), request_id VARCHAR(50), status VARCHAR(50))")

    ###getting exclude list from BMC db
    exlist = exclude_emaillist_bmc.exclude_email()
    
    while True:
        mail = imaplib.IMAP4_SSL(Constants.IMAP_HOST)
        mail.login(Constants.EMAIL_USER,base64.b64decode(Constants.EMAIL_PASSWORD))
        mail.list()
        mail.select("inbox") # read mails from inbox
    
        ###check unread mail
        result, data = mail.search(None, "UNSEEN") 
        id_list = data[0].split()

        for idl in id_list: #iterating through unread mails
            result, data = mail.fetch(idl, "(RFC822)") # fetch the email body (RFC822) for the given ID
            raw_email = data[0][1] # here's the body, which is raw text of the whole email
            
            result, threadid = mail.uid('fetch', idl, '(X-GM-THRID)') #getting threadid for a mail
            result, msgid = mail.uid('fetch', idl, '(X-GM-MSGID)') #getting messageid for a mail
            thid = threadid[0].split(' ')[2] #this is 'Email_ThreadID'
            msid = msgid[0].split(' ')[2] #messageid, this will be MESSAGE ID
            
            email_message = email.message_from_string(raw_email) #read email, this will be NOTES
            msg_from = email.utils.parseaddr(email_message['From']) #getting 'from', this will be 'From'
            msg_to = email_message['To']
            msg_cc = email_message['cc']
            msg_sub = email_message['subject'] #getting subject, this will be 'Summary'
            print "subject: " +str(msg_sub)
            
            ###check if mail is failure mail, with subject starting from 'RBE Notification'
            check_failure_mail = re.match(r'RBE Notification',msg_sub,re.M|re.I)
            if check_failure_mail:
                continue

            ###geting 'Notes'
            if email_message.is_multipart():
                for part in email_message.walk():
                    ctype = part.get_content_type()
                    cdispo = str(part.get('Content-Disposition'))
                    # skip any text/plain (txt) attachments
                    if ctype == 'text/plain' and 'attachment' not in cdispo:
                        email_body = part.get_payload(decode=True)  # decode
                        break
            #plain text, no attachments
            else:
                email_body = email_message.get_payload(decode=True)
            print "email body: " +str(email_body)

            ###get to and cc list
            new_msg_to, cc_list = get_to_cc(msg_to,msg_cc,exlist)
            

            ###checking if subject starts with REQ/WS/INC/TAS
            chck = re.match(Constants.CHCK_STRING,msg_sub,re.M|re.I)
            if(chck):    
                ###forward mail to Remedy inbox
                mail.uid('STORE', idl, '-FLAGS', '(\Seen)')
                forward_mail(email_message,msg_from,new_msg_to,msg_sub,mail,idl)
                continue

            else:
                select_stmt = "SELECT thread_id, incident_id, request_id, status FROM mail_details where thread_id = '" + thid +"';" #select details for the threadid
                rcnt = cur.execute(select_stmt)
                if(rcnt == 0): #if thread id is not present in db, insert into db
                    insert_stmt = "INSERT INTO mail_details (thread_id, incident_id, request_id, status) VALUES ('" + thid + "',NULL,NULL,NULL);"
                    cur.execute(insert_stmt)

                    ###calling webservice to get incident#
                    inc_id = create_incid_ws.create_incident_id(msg_sub,email_body,thid,msg_from,str(new_msg_to),str(cc_list))
                    if inc_id:
                        ###update db with incident id got from webservice
                        update_stmt = "UPDATE mail_details SET incident_id = '"+inc_id+"' where thread_id = '"+thid+"';"
                        cur.execute(update_stmt)

                        ###function to get request id details
                        req_id = get_request_details(inc_id,thid,cur)

                        msg_sub_with_reqid = req_id + " - " + msg_sub
                        ###forward mail to Remedy inbox, after request id is generated
                        mail.uid('STORE', idl, '-FLAGS', '(\Seen)')
                        forward_mail(email_message,msg_from,new_msg_to,msg_sub_with_reqid,mail,idl)

                        
                ###if thread id is already present in db
                else:
                    #####mail.uid('STORE', idl, '-FLAGS', '(\Seen)') #mark the mail as unread
                    select_stmt = "SELECT thread_id, incident_id, request_id FROM mail_details WHERE thread_id = '"+thid+"';"
                    cur.execute(select_stmt)
                    details = cur.fetchone()
                    if details[2] is None:
                        req_id = get_request_details(details[1],thid,cur)
                        
                        msg_sub_with_reqid = req_id + " - " + msg_sub
                        ###forward mail to Remedy inbox, after request is generated
                        mail.uid('STORE', idl, '-FLAGS', '(\Seen)')
                        forward_mail(email_message,msg_from,new_msg_to,msg_sub_with_reqid,mail,idl)
                    else:
                        msg_sub_with_reqid = details[2] + " - " + msg_sub
                        ###forward mail to Remedy inbox, after request is generated
                        mail.uid('STORE', idl, '-FLAGS', '(\Seen)')
                        forward_mail(email_message,msg_from,new_msg_to,msg_sub_with_reqid,mail,idl)
                continue
        
        #time.sleep(2)
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
