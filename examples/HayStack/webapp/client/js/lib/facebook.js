

function getUser() {
  FB.api('/me?fields=first_name,last_name,picture', function(response) {
    if (response.error) {
      console.log(response.error);

    } else {
      S.set('user', S.User.create(response));

    }
  });
}