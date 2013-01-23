passport
========

Passport service is a service to manage account, account related components and entities.

Glossary:
- User: Entity that interacts with continuuity systems: creating VPC instance, deploying code, managing dataset acls, etc
- Account: 
- DataSet: A collection of data that logically belongs together, with methods to manipulate the data. 
- VPC (Virtual private cloud): A deployment of the app fabric platform dedicated to a single customer.


Features of passport service:
   - Account registration
   - User authentication
   - Account/User management
   - Registering and un-registering account components
       * DataSets
       * VPCs
   - Managing ACLs for Account components


Design Thoughts:

         - Core Java API 
             - Implements the account, account component, account entites management
         - Thrift wrapper for the API
             - Generates Client and Server code and wraps the functionality
         - HTTP wrapper for the API
             - Implement ReST API on top of the java API to be accessed by web clients


       

Account Datamodel:
  - Account 
        - AccountName
        - AccountID
        - UserRoles: Set of Userid and Roles
           - UserID
           - Role: {Owner, Admin, User}
        - Billing info - To be updated
        - Components: Set of Components 
           - Component Type
           - Component Id
           - Component ACL: Set of UserId and ACL
              -UserID
              -ACL : {ALL, READ, READ_WRITE_DELETE}
        
User Datamodel: 
  - User
      - User ID
      - FirstName
      - LastName
      - EmailId
      - Password
