nodebb-plugin-import-smallworld
========================

a SmallWorld community forum exporter to be required by [nodebb-plugin-import](https://github.com/akhoury/nodebb-plugin-import).


# issues/questions

* users.created_at is null for everyone, we can't set the joindate in NodeBB
* users.role/membership are null for everyone, are these obfuscated? basically i can't tell if user is an admin or a moderator
* users.stats is null for everyone, is this obfuscated?
* I am assuming that users.connections are the "following/followed" in NodeBB is this a good assumptions?
* there are no banned users?
* there are no topics.pinned/locked/viewcount/tags/attachments?
