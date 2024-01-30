// DONE Retrieve the list of country names that have won a world cup. 
db.countries.distinct("cname", { $where: "this.world_cup_history.length > 1" });

/* DONE
  Retrieve the list of country names that have won a world cup 
  and the number of world cups each has won in descending order.
*/

db.countries.aggregate([{
  $match: {
    world_cup_history: {
      $exists: true,
      $ne: []
    }
  }
}, {
  $project: {
    cname: 1,
    _id: 0,
    world_cup_wins: {
      $size: '$world_cup_history'
    }
  }
},
{
  $sort: {
    world_cup_wins: -1
  }
}])

/* DONE
  List the Capital of the countries in increasing order of country population for countries 
  that have population more than 100 million. */
db.countries.aggregate([{ $match: { population: { $gt: 100 } } }, {
  $project: {
    capital: "$capital",
    population: "$population",
    _id: 0
  }
},
{
  $sort: {
    population: 1
  }
}])
/* DONE
   List the Name of the stadium which has hosted a match where the no of goals scored 
   by a single team was greater than 4. */
db.stadiums.distinct("stadium", {
  $or: [
    { "matches.team1Score": { $gt: 4 } },
    { "matches.team2Score": { $gt: 4 } }
  ]
});

/* DONE
  List the names of all the cities which have the name of the Stadium 
  starting with “Estadio”. */

db.stadiums.aggregate([{ $match: { "stadium": { "$regex": "^'Estadio" } } }, { $project: { stadium: 1, city: 1, _id: 0 } }]);

/* DONE
  List all stadiums and the number of matches hosted by each stadium. */

db.stadiums.aggregate([
  {
    $group: {
      _id: '$stadium',
      count: { $sum: 1 }
    }
  },
  {
    $project: {
      stadium: '$_id',
      matchesCount: '$count',
      _id: 0
    }
  }
]);


/* DONE
   List the First Name, Last Name, and Date of Birth of Players whose heights are 
   greater than 198 cms. */


db.countries.aggregate([
  {
    $unwind: "$players"
  },
  {
    $match: { "players.height": { $gt: 198 } }
  },
  {
    $project: {
      _id: 0,
      Fname: "$players.fname",
      Lname: "$players.lname",
      DOB: "$players.dob"
    }
  }
]);


/* List the Stadium Names and the Teams (Team1 and Team2) that played Matches 
    between 20-Jun-2014 and 24-Jun-2014 */



/* List the Fname, Lname, Position, and No of Goals scored by the Captain of a team 
  who has more than 2 Yellow cards or 1 Red card. */

