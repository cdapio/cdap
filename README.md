Passport service is a service to manage account, account related components and entities.
==========================================================================================

**Glossary:**
- User: Entity that interacts with continuuity systems: creating VPC instance, deploying code, managing dataset acls, etc
- Account: Account identifies a customer that can provision VPC, set ACLs on datasets and manage users within the accounts
- DataSet: A collection of data that logically belongs together, with methods to manipulate the data. 
- VPC (Virtual private cloud): A deployment of the app fabric platform dedicated to a single customer.


**Features of passport service:**
- Account registration
- User authentication
- Account/User management
- Registering and un-registering account components
  - DataSets
  - VPCs
- Managing ACLs for Account components


**Design Thoughts:**

- Core Java API 
	- Implements the account, account component, account entites management
- RPC wrapper for the API
	- Generates Client and Server code and wraps the functionality
- HTTP wrapper for the API
	- Implement ReST API on top of the java API to be accessed by web clients


       

**Account Datamodel:**
- Account 
	- Account Meta
	  - Account ID
    - First Name
    - Last Name
    - Email Id
    - Company ID
  - Security Credentials
    - Password
    - Password Salt
  - Billing Info
    - Credit Card Name
    - Credit Card Type
    - Credit Card cvv
    - Credit Card expiration
  - Role Definitions
    - Role Type
    - Permissions
  - VPC
    - VPC Name
    - User Roles
      - Account Id
      - Role Type
      - Role Overrides
  - Components
    - Component Name
    - Component Type
    - Component ACLs
      - Account ID
      - Permissions
